;;;
;;;   Copyright 2015, Frankfurt University of Applied Sciences
;;;
;;;   This software is released under the terms of the Eclipse Public License 
;;;   (EPL) 1.0. You can find a copy of the EPL at: 
;;;   http://opensource.org/licenses/eclipse-1.0.php
;;;

(ns
  ^{:author "Ruediger Gad",
    :doc "Main class"}
  (:use clojure.pprint
        [clojure.string :only [join split]]
        clojure.tools.cli
        clj-assorted-utils.util)
  (:require (clj-jms-activemq-toolkit [jms :as activemq]))
  (:gen-class))

(defn -main [& args]
  (let [cli-args (cli args
                      ["-d" "--daemon" "Run as daemon." :flag true :default false]
                      ["-u" "--url" 
                       "URL to connect to the broker." 
                       :default "tcp://localhost:61616"]
                      ["-t" "--topic"
                       "Name of the topic to which the data is sent."
                       :default "test.fifo.topic"]
                      ["-f" "--file"
                       "Name of the file/fifo to where the data will be written."
                       :default "test.writer.fifo"]
                      ["-h" "--help" "Print this help." :flag true])
        arg-map (cli-args 0)
        extra-args (cli-args 1)
        help-string (cli-args 2)]
    (when (arg-map :help)
      (println help-string)
      (System/exit 0))
    (let [url (arg-map :url)
          topic (str "/topic/" (arg-map :topic))
          out-file (arg-map :file)
          file-wrtr (create-string-to-file-output out-file {:append true :await-open false :insert-newline true
                                                            :resume-broken-pipe true :catch-exceptions true})
          consumer (activemq/create-consumer url topic (fn [data]
                                                         (if (= String (type data))
                                                           (file-wrtr data)
                                                           (println "Received non-string data:" data))))
          shutdown-fn (fn []
                        (consumer :close)
                        (file-wrtr))]
      (if (:daemon arg-map)
            (-> (agent 0) (await))
            (do
              (println "FIFO writer started... Type \"q\" followed by <Return> to quit: ")
              (while (not= "q" (read-line))
                (println "Type \"q\" followed by <Return> to quit: "))
              (println "Shutting down...")
              (shutdown-fn))))))

