-- Tags: no-parallel-replicas
-- no-parallel-replicas: FORMAT JSON returns additional keys
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_v;

CREATE TABLE t ( a String ) ENGINE = Memory();
CREATE VIEW t_v AS SELECT * FROM t;
SET output_format_write_statistics = 0;
SELECT * FROM t_v FORMAT JSON SETTINGS extremes = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_v;
