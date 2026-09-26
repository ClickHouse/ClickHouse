-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-fasttest
-- no-fasttest: UNIQUE KEY INSERT writes the dense-index SST, which needs RocksDB.
-- UNIQUE KEY interim guard: BACKUP is rejected on a table with a UNIQUE KEY. Not because the
-- sidecars would be omitted -- they reach the backup now -- but because restore renames every
-- part while a bitmap names its target in its file name. All keys distinct.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_guard;

CREATE TABLE uk_guard (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

INSERT INTO uk_guard VALUES (1, 'a'), (2, 'b'), (3, 'c');

BACKUP TABLE uk_guard TO Null FORMAT Null; -- { serverError SUPPORT_IS_DISABLED }

-- CHECK TABLE is NOT rejected any more. Asserting only that it passes would prove nothing: a
-- bitmap with no checksum entry is declared by `getFileNamesWithoutChecksums` and skipped just as
-- quietly. Only corrupting one tells the two apart, which needs the write paths that stage one.

-- A plain MergeTree table (no UNIQUE KEY) is unaffected: CHECK still works.
DROP TABLE IF EXISTS plain_guard;
CREATE TABLE plain_guard (id UInt64, v String) ENGINE = MergeTree ORDER BY (id);
INSERT INTO plain_guard VALUES (1, 'a'), (2, 'b');
CHECK TABLE plain_guard FORMAT Null;

DROP TABLE uk_guard;
DROP TABLE plain_guard;
