-- Tags: no-shared-merge-tree
-- Stop replication queues
DROP TABLE IF EXISTS wrong_metadata;

CREATE TABLE wrong_metadata(
    column1 UInt64,
    column2 UInt64,
    column3 UInt64
)
ENGINE ReplicatedMergeTree('/test/{database}/tables/wrong_metadata', '1')
ORDER BY tuple();

INSERT INTO wrong_metadata VALUES (1, 2, 3);

SYSTEM STOP REPLICATION QUEUES wrong_metadata;

ALTER TABLE wrong_metadata RENAME COLUMN column1 TO column1_renamed SETTINGS replication_alter_partitions_sync = 0;

INSERT INTO wrong_metadata VALUES (4, 5, 6);

SELECT * FROM wrong_metadata ORDER BY column1;

SYSTEM START REPLICATION QUEUES wrong_metadata;

SYSTEM SYNC REPLICA wrong_metadata;

ALTER TABLE wrong_metadata RENAME COLUMN column2 to column2_renamed SETTINGS replication_alter_partitions_sync = 2;

SELECT * FROM wrong_metadata ORDER BY column1_renamed FORMAT JSONEachRow;

DROP TABLE IF EXISTS wrong_metadata;


CREATE TABLE wrong_metadata_wide(
    column1 UInt64,
    column2 UInt64,
    column3 UInt64
)
ENGINE ReplicatedMergeTree('/test/{database}/tables/wrong_metadata_wide', '1')
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO wrong_metadata_wide VALUES (1, 2, 3);

SYSTEM STOP REPLICATION QUEUES wrong_metadata_wide;

ALTER TABLE wrong_metadata_wide RENAME COLUMN column1 TO column1_renamed SETTINGS replication_alter_partitions_sync = 0;

INSERT INTO wrong_metadata_wide VALUES (4, 5, 6);

SELECT * FROM wrong_metadata_wide ORDER by column1;

SYSTEM START REPLICATION QUEUES wrong_metadata_wide;

SYSTEM SYNC REPLICA wrong_metadata_wide;

ALTER TABLE wrong_metadata_wide RENAME COLUMN column2 to column2_renamed SETTINGS replication_alter_partitions_sync = 2;

SELECT * FROM wrong_metadata_wide ORDER BY column1_renamed FORMAT JSONEachRow;

DROP TABLE IF EXISTS wrong_metadata_wide;
