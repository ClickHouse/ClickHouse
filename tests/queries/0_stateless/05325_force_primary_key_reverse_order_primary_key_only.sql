-- `force_primary_key_reverse_order` must also reverse the sorting key of a `MergeTree` table
-- defined with `PRIMARY KEY` only, which is implicitly used as `ORDER BY`.

SET force_primary_key_reverse_order = 1;

DROP TABLE IF EXISTS t_pk_only_single;
DROP TABLE IF EXISTS t_pk_only_multi;

CREATE TABLE t_pk_only_single (k UInt64, v String) ENGINE = MergeTree PRIMARY KEY k;
CREATE TABLE t_pk_only_multi (a UInt64, b UInt64) ENGINE = MergeTree PRIMARY KEY (a, b);

SELECT name, extract(create_table_query, 'ORDER BY (.*) SETTINGS'), primary_key FROM system.tables
WHERE database = currentDatabase() AND name LIKE 't_pk_only_%' ORDER BY name;

INSERT INTO t_pk_only_single SELECT number, toString(number) FROM numbers(10);
INSERT INTO t_pk_only_multi SELECT number % 3, number FROM numbers(9);

SELECT k FROM t_pk_only_single ORDER BY k DESC LIMIT 3;
SELECT k FROM t_pk_only_single WHERE k BETWEEN 3 AND 5 ORDER BY k;
SELECT a, b FROM t_pk_only_multi ORDER BY a DESC, b DESC LIMIT 4;

DROP TABLE t_pk_only_single;
DROP TABLE t_pk_only_multi;
