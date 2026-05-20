-- `WITH CLUSTER` on a tuple-typed column is not supported — only inline
-- tuple expressions like `(x, y)` open the 2D path. Verify the explicit
-- rejection and the suggested unpacking workaround.

DROP TABLE IF EXISTS t_cluster_tuple;
CREATE TABLE t_cluster_tuple (p Tuple(Float64, Float64), v UInt8) ENGINE = Memory;
INSERT INTO t_cluster_tuple VALUES ((0.0, 0.0), 1), ((1.0, 1.0), 2), ((10.0, 10.0), 3);

-- Direct tuple-typed column: must be rejected with a clear message.
SELECT sum(v) FROM t_cluster_tuple GROUP BY p WITH CLUSTER 2; -- { serverError BAD_ARGUMENTS }

-- Workaround: unpack the tuple via `p.1, p.2` — chain merges {(0,0), (1,1)}
-- into one cluster, leaves {(10,10)} as a singleton.
SELECT count() AS num_clusters
FROM (
    SELECT sum(v) FROM t_cluster_tuple GROUP BY (p.1, p.2) WITH CLUSTER 2
);

DROP TABLE t_cluster_tuple;
