DROP TABLE IF EXISTS t_enum;
DROP TABLE IF EXISTS t_source;

CREATE TABLE t_enum(x Enum8('hello' = 1, 'world' = 2)) ENGINE = TinyLog;
CREATE TABLE t_source(x Nullable(String)) ENGINE = TinyLog;

INSERT INTO t_source (x) VALUES ('hello');
INSERT INTO t_enum(x) SELECT x from t_source WHERE x in ('hello', 'world');
SELECT * FROM t_enum;

DROP TABLE IF EXISTS t_enum;
DROP TABLE IF EXISTS t_source;
