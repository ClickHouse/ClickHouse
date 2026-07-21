(ns jepsen.clickhouse.server.client
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer :all]
            [jepsen.util :as util]
            [jepsen.reconnect :as rc]))

(def operation-timeout "Default operation timeout in ms" 10000)

(defn db-spec
  [node]
  {:dbtype "clickhouse"
   :dbname "default"
   :classname "com.clickhouse.ClickhouseDriver"
   :host (name node)
   :port 8123
   :connectTimeout 30
   :socketTimeout 30
   :jdbcCompliant false})

(defn open-connection
  [node]
   (util/timeout 30000
               (throw (RuntimeException.
                        (str "Connection to " node " timed out")))
    (util/retry 0.1
      (let [spec (db-spec node)
            connection (j/get-connection spec)
            added-connection (j/add-connection spec connection)]
        (assert added-connection)
        added-connection))))

(defn close-connection
  "Close connection"
  [connection]
  (when-let [c (j/db-find-connection connection)]
    (.close c))
  (dissoc connection :connection))

(defn client
  "Client JDBC"
  [node]
  (rc/open!
    (rc/wrapper
      {:name (name node)
       :open (partial open-connection node)
       :close close-connection
       :log? true})))

(defmacro with-connection
  "Like jepsen.reconnect/with-conn, but also asserts that the connection has
  not been closed. If it has, throws an ex-info with :type :conn-not-ready.
  Delays by 1 second to allow time for the DB to recover."
  [[c client] final & body]
  `(do
     (when ~final
      (rc/reopen! ~client))
     (rc/with-conn [~c ~client]
       (when (.isClosed (j/db-find-connection ~c))
         (Thread/sleep 1000)
         (throw (ex-info "Connection not yet ready."
                         {:type :conn-not-ready})))
       ~@body)))

(defmacro with-exception
  "Takes an operation and a body. Evaluates body, catches exceptions, and maps
  them to ops with :type :info and a descriptive :error."
  [op & body]
  `(try ~@body
        (catch Exception e#
          (if-let [message# (.getMessage e#)]
            (assoc ~op :type :fail, :error message#)
            (throw e#)))))
