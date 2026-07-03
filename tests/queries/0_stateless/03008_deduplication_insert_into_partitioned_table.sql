DROP TABLE IF EXISTS partitioned_table;
DROP TABLE IF EXISTS mv_table;


SET deduplicate_blocks_in_dependent_materialized_views = 1;


SELECT 'no user deduplication token';

CREATE TABLE partitioned_table
    (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table', '{replica}')
    partition by key % 10
    order by tuple();

CREATE MATERIALIZED VIEW mv_table (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table_mv', '{replica}')
    ORDER BY tuple()
    AS SELECT key, value FROM partitioned_table;

INSERT INTO partitioned_table VALUES (1, 'A'), (2, 'B');
INSERT INTO partitioned_table VALUES (1, 'A'), (2, 'C');
INSERT INTO partitioned_table VALUES (1, 'D'), (2, 'B');

SELECT 'partitioned_table is deduplicated bacause deduplication works in scope of one partiotion:';
SELECT * FROM partitioned_table ORDER BY ALL;
SELECT 'mv_table is not deduplicated because the inserted blocks was different:';
SELECT * FROM mv_table ORDER BY ALL;

DROP TABLE partitioned_table;
DROP TABLE mv_table;


SELECT 'with user deduplication token';

CREATE TABLE partitioned_table
    (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table', '{replica}')
    partition by key % 10
    order by tuple();

CREATE MATERIALIZED VIEW mv_table (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table_mv', '{replica}')
    ORDER BY tuple()
    AS SELECT key, value FROM partitioned_table;

INSERT INTO partitioned_table SETTINGS insert_deduplication_token='token_1' VALUES (1, 'A'), (2, 'B');
INSERT INTO partitioned_table SETTINGS insert_deduplication_token='token_2' VALUES (1, 'A'), (2, 'C');
INSERT INTO partitioned_table SETTINGS insert_deduplication_token='token_3' VALUES (1, 'D'), (2, 'B');

SELECT 'partitioned_table is not deduplicated because different tokens:';
SELECT * FROM partitioned_table ORDER BY ALL;
SELECT 'mv_table is not deduplicated because different tokens:';
SELECT * FROM mv_table ORDER BY ALL;

DROP TABLE partitioned_table;
DROP TABLE mv_table;


SELECT 'with incorrect usage of user deduplication token';

CREATE TABLE partitioned_table
    (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table', '{replica}')
    partition by key % 10
    order by tuple();

CREATE MATERIALIZED VIEW mv_table (key Int64, value String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_insert_into_partitioned_table_mv', '{replica}')
    ORDER BY tuple()
    AS SELECT key, value FROM partitioned_table;

INSERT INTO partitioned_table SETTINGS insert_deduplication_token='token_0' VALUES (1, 'A'), (2, 'B');
INSERT INTO partitioned_table SETTINGS insert_deduplication_token='token_0' VALUES (1, 'A'), (2, 'C');
INSERT INTO partitioned_table SETTINGS insert_deduplication_token='token_0' VALUES (1, 'D'), (2, 'B');

SELECT 'partitioned_table is deduplicated because equal tokens:';
SELECT * FROM partitioned_table ORDER BY ALL;
SELECT 'mv_table is deduplicated because equal tokens:';
SELECT * FROM mv_table ORDER BY ALL;

DROP TABLE partitioned_table;
DROP TABLE mv_table;
