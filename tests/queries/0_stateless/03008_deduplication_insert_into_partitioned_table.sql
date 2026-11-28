DROP TABLE IF EXISTS partitioned_table_1;
DROP TABLE IF EXISTS mv_table_1;
DROP TABLE IF EXISTS partitioned_table_2;
DROP TABLE IF EXISTS mv_table_2;
DROP TABLE IF EXISTS partitioned_table_3;
DROP TABLE IF EXISTS mv_table_3;


SET deduplicate_blocks_in_dependent_materialized_views = 1;


SELECT 'no user deduplication token';

CREATE TABLE partitioned_table_1
    (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table_1', '{replica}')
    partition by key % 10
    order by tuple();

CREATE MATERIALIZED VIEW mv_table_1 (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table_1_mv', '{replica}')
    ORDER BY tuple()
    AS SELECT key, value FROM partitioned_table_1;

INSERT INTO partitioned_table_1 VALUES (1, 'A'), (2, 'B');
INSERT INTO partitioned_table_1 VALUES (1, 'A'), (2, 'C');
INSERT INTO partitioned_table_1 VALUES (1, 'D'), (2, 'B');

SELECT 'partitioned_table is not deduplicated because the inserts are different (deduplication works per insert):';
SELECT * FROM partitioned_table_1 ORDER BY ALL;
SELECT 'mv_table is not deduplicated because the inserted blocks was different:';
SELECT * FROM mv_table_1 ORDER BY ALL;

DROP TABLE partitioned_table_1;
DROP TABLE mv_table_1;


SELECT 'with user deduplication token';

CREATE TABLE partitioned_table_2
    (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table_2', '{replica}')
    partition by key % 10
    order by tuple();

CREATE MATERIALIZED VIEW mv_table_2 (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table_2_mv', '{replica}')
    ORDER BY tuple()
    AS SELECT key, value FROM partitioned_table_2;

INSERT INTO partitioned_table_2 SETTINGS insert_deduplication_token='token_1' VALUES (1, 'A'), (2, 'B');
INSERT INTO partitioned_table_2 SETTINGS insert_deduplication_token='token_2' VALUES (1, 'A'), (2, 'C');
INSERT INTO partitioned_table_2 SETTINGS insert_deduplication_token='token_3' VALUES (1, 'D'), (2, 'B');

SELECT 'partitioned_table is not deduplicated because different tokens:';
SELECT * FROM partitioned_table_2 ORDER BY ALL;
SELECT 'mv_table is not deduplicated because different tokens:';
SELECT * FROM mv_table_2 ORDER BY ALL;

DROP TABLE partitioned_table_2;
DROP TABLE mv_table_2;


SELECT 'with incorrect usage of user deduplication token';

CREATE TABLE partitioned_table_3
    (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table_3', '{replica}')
    partition by key % 10
    order by tuple();

CREATE MATERIALIZED VIEW mv_table_3 (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table_3_mv', '{replica}')
    ORDER BY tuple()
    AS SELECT key, value FROM partitioned_table_3;

INSERT INTO partitioned_table_3 SETTINGS insert_deduplication_token='token_0' VALUES (1, 'A'), (2, 'B');
INSERT INTO partitioned_table_3 SETTINGS insert_deduplication_token='token_0' VALUES (1, 'A'), (2, 'C');
INSERT INTO partitioned_table_3 SETTINGS insert_deduplication_token='token_0' VALUES (1, 'D'), (2, 'B');

SELECT 'partitioned_table is deduplicated because equal tokens:';
SELECT * FROM partitioned_table_3 ORDER BY ALL;
SELECT 'mv_table is deduplicated because equal tokens:';
SELECT * FROM mv_table_3 ORDER BY ALL;

DROP TABLE partitioned_table_3;
DROP TABLE mv_table_3;
