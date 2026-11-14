(ns jepsen.control.sshj
  (:require [jepsen.control [core :as core]
                            [sshj :as sshj]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (net.schmizz.sshj SSHClient
                            DefaultConfig)
           (net.schmizz.sshj.transport.verification PromiscuousVerifier)
           (java.util.concurrent Semaphore)))

(defrecord SSHJRemote [concurrency-limit
                       conn-spec
                       ^SSHClient client
                       ^Semaphore semaphore]
  core/Remote
  (connect [this conn-spec]
    (if (:dummy conn-spec)
      (assoc this :conn-spec conn-spec)
      (try+ (let [c (as-> (SSHClient.) client
                      (do
                        (if (:strict-host-key-checking conn-spec)
                          (.loadKnownHosts client)
                          (.addHostKeyVerifier client (PromiscuousVerifier.)))
                        (.connect client (:host conn-spec) (:port conn-spec))
                        (auth! client conn-spec)
                        client))]
              (assoc this
                     :conn-spec conn-spec
                     :client c
                     :semaphore (Semaphore. concurrency-limit true)))
            (catch Exception e
              ; SSHJ wraps InterruptedException in its own exceptions, so we
              ; have to see through that and rethrow properly.
              (let [cause (util/ex-root-cause e)]
                (when (instance? InterruptedException cause)
                  (throw cause)))
              (throw+ (assoc conn-spec
                             :type    :jepsen.control/session-error
                             :message "Error opening SSH session. Verify username, password, and node hostnames are correct."))))))

  (disconnect! [this]
    (when-let [c client]
      (.close c)))

  (execute! [this ctx action]
    ;  (info :permits (.availablePermits semaphore))
    (when (:dummy conn-spec)
      (throw+ {:type :jepsen.control/dummy}))
    (.acquire semaphore)
    (sshj/with-errors conn-spec ctx
      (try
        (with-open [session (.startSession client)]
          (let [cmd (.exec session (:cmd action))
                ; Feed it input
                _ (when-let [input (:in action)]
                    (let [stream (.getOutputStream cmd)]
                      (bs/transfer input stream)
                      (send-eof! client session)
                      (.close stream)))
                ; Read output
                out (.toString (IOUtils/readFully (.getInputStream cmd)))
                err (.toString (IOUtils/readFully (.getErrorStream cmd)))
                ; Wait on command
                _ (.join cmd)]
            ; Return completion
            (assoc action
                   :out   out
                   :err   err
                   ; There's also a .getExitErrorMessage that might be
                   ; interesting here?
                   :exit  (.getExitStatus cmd))))
        (finally
          (.release semaphore)))))

  (upload! [this ctx local-paths remote-path _opts]
    (when (:dummy conn-spec)
      (throw+ {:type :jepsen.control/dummy}))
    (with-errors conn-spec ctx
      (with-open [sftp (.newSFTPClient client)]
        (.put sftp (FileSystemFile. local-paths) remote-path))))

  (download! [this ctx remote-paths local-path _opts]
    (when (:dummy conn-spec)
      (throw+ {:type :jepsen.control/dummy}))
    (with-errors conn-spec ctx
      (with-open [sftp (.newSFTPClient client)]
        (.get sftp remote-paths (FileSystemFile. local-path))))))

(defn remote
  "Constructs an SSHJ remote."
  []
  (-> (SSHJRemote. concurrency-limit nil nil nil)
      ; We *can* use our own SCP, but shelling out is faster.
      scp/remote
      retry/remote))

(ns jepsen.clickhouse.main
  (:require [jepsen.clickhouse.keeper.main]
            [jepsen.clickhouse.server.main]))

(defn -main
  [f & args]
  (cond
   (= f "keeper") (apply jepsen.clickhouse.keeper.main/main args)
   (= f "server") (apply jepsen.clickhouse.server.main/main args)
   (some #(= f %) ["test" "test-all"]) (apply jepsen.clickhouse.keeper.main/main f args) ;; backwards compatibility
   :unknown (throw (Exception. (str "Unknown option specified: " f)))))
