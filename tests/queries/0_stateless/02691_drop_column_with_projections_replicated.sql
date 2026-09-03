DROP TABLE IF EXISTS 02691_drop_column_replicated;

CREATE TABLE 02691_drop_column_replicated (col1 Int64, col2 Int64, PROJECTION 02691_drop_column_replicated (SELECT * ORDER BY col1 ))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/02691_drop_column', 'r1')
ORDER BY col1;

INSERT INTO 02691_drop_column_replicated VALUES (1, 2);

ALTER TABLE 02691_drop_column_replicated DROP COLUMN col2 SETTINGS alter_sync = 2;

DROP TABLE 02691_drop_column_replicated;
