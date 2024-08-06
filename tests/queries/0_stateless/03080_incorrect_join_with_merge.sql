-- https://github.com/ClickHouse/ClickHouse/issues/29838
SET enable_analyzer=1;
SET distributed_foreground_insert=1;

CREATE TABLE first_table_lr
(
    id String,
    id2 String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_03080/alter', 'r1')
ORDER BY id;


CREATE TABLE first_table
(
    id String,
    id2 String
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'first_table_lr');


CREATE TABLE second_table_lr
(
    id String,
    id2 String
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE second_table
(
    id String,
    id2 String
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'second_table_lr');

INSERT INTO first_table VALUES ('1', '2'), ('3', '4');
INSERT INTO second_table VALUES ('1', '2'), ('3', '4');

CREATE TABLE two_tables
(
    id String,
    id2 String
)
ENGINE = Merge(currentDatabase(), '^(first_table)$');

SELECT
    count()
FROM first_table as s
GLOBAL ANY JOIN second_table as f USING (id)
WHERE
    f.id2 GLOBAL IN (
        SELECT
            id2
        FROM second_table
        GROUP BY id2
    );

SELECT
    count()
FROM two_tables as s
GLOBAL ANY JOIN second_table as f USING (id)
WHERE
    f.id2 GLOBAL IN (
        SELECT
            id2
        FROM second_table
        GROUP BY id2
    );
