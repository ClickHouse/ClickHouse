-- insert data duplicates by providing deduplication token on insert

DROP TABLE IF EXISTS replicated_with_duplicates SYNC;

CREATE TABLE IF NOT EXISTS replicated_with_duplicates (
    id Int32, val UInt32
) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_02115/replicated_with_duplicates', '{replica}') ORDER BY id;

-- only one row will be inserted
INSERT INTO replicated_with_duplicates VALUES(1, 1001);
INSERT INTO replicated_with_duplicates VALUES(1, 1001);

SELECT * FROM replicated_with_duplicates;

select 'insert duplicate by providing deduplication token';
set insert_deduplication_token = '1';
INSERT INTO replicated_with_duplicates VALUES(1, 1001);

OPTIMIZE TABLE replicated_with_duplicates;
SELECT * FROM replicated_with_duplicates;
