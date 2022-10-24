(ns jepsen.clickhouse.server.client
  (:require [clojure.java.jdbc :as j]
            [jepsen.reconnect :as rc]))

(defn db-spec
  [node]
  {:dbtype "clickhouse"
   :dbname "default"
   :classname "com.clickhouse.ClickhouseDriver"
   :host (name node)
   :port 8123
   :jdbcCompliant false})

(defn open-connection
  [node]
  (let [spec (db-spec node)
        connection (j/get-connection spec)
        added-connection (j/add-connection spec connection)]
    (assert added-connection)
    added-connection))

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
       :open #(open-connection node)
       :close close-connection
       :log? true})))

(defmacro with-connection
  "Like jepsen.reconnect/with-conn, but also asserts that the connection has
  not been closed. If it has, throws an ex-info with :type :conn-not-ready.
  Delays by 1 second to allow time for the DB to recover."
  [[c client] & body]
  `(rc/with-conn [~c ~client]
     (when (.isClosed (j/db-find-connection ~c))
       (Thread/sleep 1000)
       (throw (ex-info "Connection not yet ready."
                       {:type :conn-not-ready})))
     ~@body))
