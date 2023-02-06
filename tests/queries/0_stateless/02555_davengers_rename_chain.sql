DROP TABLE IF EXISTS wrong_metadata;

CREATE TABLE wrong_metadata(
    a UInt64,
    b UInt64,
    c UInt64
)
ENGINE ReplicatedMergeTree('/test/{database}/tables/wrong_metadata', '1')
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO wrong_metadata VALUES (1, 2, 3);

SYSTEM STOP MERGES wrong_metadata;

ALTER TABLE wrong_metadata RENAME COLUMN a TO a1, RENAME COLUMN b to b1 SETTINGS replication_alter_partitions_sync = 0;

SELECT sleep(1) FORMAT Null;

SELECT * FROM wrong_metadata ORDER BY a1 FORMAT JSONEachRow;
SELECT '~~~~~~~';

INSERT INTO wrong_metadata VALUES (4, 5, 6);

SELECT * FROM wrong_metadata ORDER BY a1 FORMAT JSONEachRow;
SELECT '~~~~~~~';

ALTER TABLE wrong_metadata RENAME COLUMN a1 TO b, RENAME COLUMN b1 to a SETTINGS replication_alter_partitions_sync = 0;

SELECT sleep(1) FORMAT Null;
SELECT sleep(1) FORMAT Null;

INSERT INTO wrong_metadata VALUES (7, 8, 9);

SELECT * FROM wrong_metadata ORDER by a1 FORMAT JSONEachRow;
SELECT '~~~~~~~';

SYSTEM START MERGES wrong_metadata;

SYSTEM SYNC REPLICA wrong_metadata;

SELECT * FROM wrong_metadata order by a FORMAT JSONEachRow;
SELECT '~~~~~~~';

DROP TABLE IF EXISTS wrong_metadata;

DROP TABLE IF EXISTS wrong_metadata_compact;

CREATE TABLE wrong_metadata_compact(
    a UInt64,
    b UInt64,
    c UInt64
)
ENGINE ReplicatedMergeTree('/test/{database}/tables/wrong_metadata_compact', '1')
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 10000000;

INSERT INTO wrong_metadata_compact VALUES (1, 2, 3);

SYSTEM STOP MERGES wrong_metadata_compact;

ALTER TABLE wrong_metadata_compact RENAME COLUMN a TO a1, RENAME COLUMN b to b1 SETTINGS replication_alter_partitions_sync = 0;

SELECT sleep(1) FORMAT Null;

SELECT * FROM wrong_metadata_compact ORDER BY a1 FORMAT JSONEachRow;
SELECT '~~~~~~~';

INSERT INTO wrong_metadata_compact VALUES (4, 5, 6);

SELECT * FROM wrong_metadata_compact ORDER BY a1 FORMAT JSONEachRow;
SELECT '~~~~~~~';

ALTER TABLE wrong_metadata_compact RENAME COLUMN a1 TO b, RENAME COLUMN b1 to a SETTINGS replication_alter_partitions_sync = 0;

SELECT sleep(1) FORMAT Null;
SELECT sleep(1) FORMAT Null;

INSERT INTO wrong_metadata_compact VALUES (7, 8, 9);

SELECT * FROM wrong_metadata_compact ORDER by a1 FORMAT JSONEachRow;
SELECT '~~~~~~~';

SYSTEM START MERGES wrong_metadata_compact;

SYSTEM SYNC REPLICA wrong_metadata_compact;

SELECT * FROM wrong_metadata_compact order by a FORMAT JSONEachRow;
SELECT '~~~~~~~';

DROP TABLE IF EXISTS wrong_metadata_compact;
