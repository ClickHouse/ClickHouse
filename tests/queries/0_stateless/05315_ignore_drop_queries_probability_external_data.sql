-- Tags: no-fasttest
-- no-fasttest: needs S3 (MinIO).

-- A DROP skipped by `ignore_drop_queries_probability` leaves the data of tables that do not hold their rows in the server.
CREATE TABLE t_s3 (x UInt64) ENGINE = S3(s3_conn, filename = currentDatabase() || '_skipped_drop_s3.tsv', format = TSV);
INSERT INTO t_s3 SETTINGS s3_truncate_on_insert = 1 VALUES (1);
DROP TABLE t_s3 SETTINGS ignore_drop_queries_probability = 1;
SELECT * FROM t_s3;

CREATE TABLE t_alias_target (x UInt64) ENGINE = S3(s3_conn, filename = currentDatabase() || '_skipped_drop_alias.tsv', format = TSV);
INSERT INTO t_alias_target SETTINGS s3_truncate_on_insert = 1 VALUES (2);
CREATE TABLE t_alias ENGINE = Alias(t_alias_target);
DROP TABLE t_alias SETTINGS ignore_drop_queries_probability = 1;
SELECT * FROM t_alias_target;

INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_skipped_drop_proxy.tsv', format = TSV, structure = 'x UInt64')
    SETTINGS s3_truncate_on_insert = 1 VALUES (3);
CREATE TABLE t_proxy (x UInt64) AS s3(s3_conn, filename = currentDatabase() || '_skipped_drop_proxy.tsv', format = TSV);
DROP TABLE t_proxy SETTINGS ignore_drop_queries_probability = 1;
SELECT * FROM t_proxy;

DROP TABLE t_proxy;
DROP TABLE t_alias;
DROP TABLE t_alias_target;
DROP TABLE t_s3;
