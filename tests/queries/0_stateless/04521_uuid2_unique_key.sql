-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- no-fasttest: UNIQUE KEY INSERT writes the dense-index SST, which needs RocksDB.
-- `UNIQUE KEY` tables with `UUID2` key columns.
-- `UUID2` reuses `ColumnVector<UUID>` storage, so a `UUID2` column reports the physical `UUID`
-- column type to the order-preserving UNIQUE KEY encoder (`UniqueKeyEncoding::appendColumn`
-- switches on `IColumn::getDataType()`), and is handled by the same big-endian branch as `UUID`.
-- This end-to-end test guards that a `UNIQUE KEY` table accepts `UUID2` key columns (both
-- explicit and materialized from a bare `UUID` under `uuid_type_version = 2`): CREATE, INSERT
-- and read-back in canonical order all work.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_uuid2;
DROP TABLE IF EXISTS uk_uuid2_composite;
DROP TABLE IF EXISTS uk_uuid2_from_version;

-- 1. Explicit `UUID2` unique key: INSERT encodes the key column and reads back the
-- canonical textual values in lexicographic order.
CREATE TABLE uk_uuid2 (id UUID2, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

INSERT INTO uk_uuid2 VALUES
    ('00000000-0000-0000-0000-000000000001', 'a'),
    ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'b'),
    ('ffffffff-ffff-ffff-ffff-ffffffffffff', 'c');

SELECT '-- explicit UUID2 unique key: rows read back in canonical order';
SELECT id, v FROM uk_uuid2 ORDER BY id;

-- 2. Composite unique key with a `UUID2` column plus a scalar column.
CREATE TABLE uk_uuid2_composite (id UUID2, user_id UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id, user_id)
ORDER BY (id, user_id);

INSERT INTO uk_uuid2_composite VALUES
    ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 10, 'x'),
    ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 20, 'y');

SELECT '-- composite UUID2 unique key';
SELECT id, user_id, v FROM uk_uuid2_composite ORDER BY id, user_id;

-- 3. Bare `UUID` under `uuid_type_version = 2` materializes to `UUID2`, so its unique-key
-- column takes the same encoding path.
SET uuid_type_version = 2;

CREATE TABLE uk_uuid2_from_version (id UUID, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SELECT '-- bare UUID under uuid_type_version = 2 stores UUID2 in the unique key';
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'uk_uuid2_from_version' AND name = 'id';

INSERT INTO uk_uuid2_from_version VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'z');
SELECT id, v FROM uk_uuid2_from_version ORDER BY id;

DROP TABLE uk_uuid2;
DROP TABLE uk_uuid2_composite;
DROP TABLE uk_uuid2_from_version;
