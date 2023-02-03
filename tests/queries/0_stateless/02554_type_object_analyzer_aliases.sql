SET allow_experimental_object_type = 1;
SET allow_experimental_analyzer = 1;

-- NOTE: those queries crash the server with old analyzer,
-- but it will be deprecated soon.

DROP TABLE IF EXISTS t_json_table_aliases;

CREATE TABLE t_json_table_aliases(a UInt64, t1_00816 JSON) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_json_table_aliases format JSONEachRow {"a": 2, "t1_00816": {"foo": 1, "k2": 2}};

SELECT t1.t1_00816, t2.t1_00816, t3.t1_00816 FROM t_json_table_aliases AS t1, t_json_table_aliases AS t2, t_json_table_aliases AS t3;

DROP TABLE t_json_table_aliases;
DROP TABLE IF EXISTS t_json_join;

CREATE TABLE t_json_join(t UInt64, flag Object(Nullable('JSON'))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_join FORMAT JSONEachRow {"t": 1, "flag": {"k1": 1, "k2" : 2}} {"t": 2, "flag": {"k2": 3, "k3" : 4}};

SELECT *
FROM t_json_join AS t1
INNER JOIN t_json_join AS t2 ON t1.t = t2.t
INNER JOIN t_json_join AS t3 ON t2.t = t3.t
ORDER BY t1.t ASC;

DROP TABLE t_json_join;
