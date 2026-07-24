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
