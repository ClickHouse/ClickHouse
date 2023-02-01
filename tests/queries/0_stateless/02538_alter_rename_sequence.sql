DROP TABLE IF EXISTS wrong_metadata;

CREATE TABLE wrong_metadata(
    column1 UInt64,
    column2 UInt64,
    column3 UInt64
)
ENGINE ReplicatedMergeTree('/test/tables/wrong_metadata', '1')
ORDER BY tuple();

INSERT INTO wrong_metadata VALUES (1, 2, 3);

SYSTEM STOP REPLICATION QUEUES wrong_metadata;

ALTER TABLE wrong_metadata RENAME COLUMN column1 TO column1_renamed SETTINGS replication_alter_partitions_sync = 0;

INSERT INTO wrong_metadata VALUES (4, 5, 6);

SYSTEM START REPLICATION QUEUES wrong_metadata;

SYSTEM SYNC REPLICA wrong_metadata;

ALTER TABLE wrong_metadata RENAME COLUMN column2 to column2_renamed SETTINGS replication_alter_partitions_sync = 2;

SELECT * FROM wrong_metadata ORDER BY column1_renamed FORMAT JSONEachRow;

DROP TABLE IF EXISTS wrong_metadata;
