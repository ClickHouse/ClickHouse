-- A part written before a metadata-only `T` -> `Nullable(T)` ALTER stores non-nullable data and
-- has no `.null` substream, so the null map must be derived from the parent column that IS in the
-- part. For a DOTTED Nested member the requested `arr.n.null` is remapped by
-- `Nested::convertToSubcolumns` into a subcolumn `n.null` of `arr`, so its `getNameInStorage()`
-- becomes `arr` while the part supplies `arr.n`. Resolving the parent from metadata is therefore
-- required; comparing the remapped spelling directly default-fills the column to all-NULL.
--
-- The mutation is left UNAPPLIED on purpose (`SYSTEM STOP MERGES` before a `mutations_sync = 0`
-- ALTER). A synchronous ALTER rewrites the part, materialises the `.null` substream and makes
-- every arm below a control.

DROP TABLE IF EXISTS t_dotted_null_wide;
DROP TABLE IF EXISTS t_dotted_null_compact;
DROP TABLE IF EXISTS t_dotted_null_no_share;
DROP TABLE IF EXISTS t_plain_null_wide;

CREATE TABLE t_dotted_null_wide (id UInt8, `arr.n` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_dotted_null_wide VALUES (1, ['a', 'b']);
SYSTEM STOP MERGES t_dotted_null_wide;
ALTER TABLE t_dotted_null_wide
    MODIFY COLUMN `arr.n` Array(Nullable(String)) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'wide, part state';
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_dotted_null_wide' AND active;
SELECT type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_dotted_null_wide' AND active AND column = 'arr.n';
SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 't_dotted_null_wide' AND name = 'arr.n';

SELECT 'wide, dotted Nested member';
SELECT `arr.n`, `arr.n`.null FROM t_dotted_null_wide;
SELECT arrayMap(v -> v IS NULL, `arr.n`) FROM t_dotted_null_wide;
SELECT count() FROM t_dotted_null_wide WHERE `arr.n`[1] IS NULL;

CREATE TABLE t_dotted_null_compact (id UInt8, `arr.n` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, auto_statistics_types = '';

INSERT INTO t_dotted_null_compact VALUES (1, ['a', 'b']);
SYSTEM STOP MERGES t_dotted_null_compact;
ALTER TABLE t_dotted_null_compact
    MODIFY COLUMN `arr.n` Array(Nullable(String)) SETTINGS mutations_sync = 0, alter_sync = 0;

-- Compact parts take a different reader: `part_columns` collects Nested only for wide parts
-- (`IMergeTreeReader`), while the remapped list reaches `fillMissingColumns` either way.
SELECT 'compact, part state';
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_dotted_null_compact' AND active;
SELECT type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_dotted_null_compact' AND active AND column = 'arr.n';

SELECT 'compact, dotted Nested member';
SELECT `arr.n`, `arr.n`.null FROM t_dotted_null_compact;
SELECT arrayMap(v -> v IS NULL, `arr.n`) FROM t_dotted_null_compact;

-- Control: with the offsets sharing off there is no remap, so the plain spelling already names
-- the parent and this arm reads the same however the parent is resolved.
CREATE TABLE t_dotted_null_no_share (id UInt8, `arr.n` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_dotted_null_no_share VALUES (1, ['a', 'b']);
SYSTEM STOP MERGES t_dotted_null_no_share;
ALTER TABLE t_dotted_null_no_share
    MODIFY COLUMN `arr.n` Array(Nullable(String)) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'control, share_nested_offsets = 0';
SELECT `arr.n`, `arr.n`.null FROM t_dotted_null_no_share;

-- Control: an undotted column is never remapped.
CREATE TABLE t_plain_null_wide (id UInt8, plain Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_plain_null_wide VALUES (1, ['a', 'b']);
SYSTEM STOP MERGES t_plain_null_wide;
ALTER TABLE t_plain_null_wide
    MODIFY COLUMN plain Array(Nullable(String)) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'control, undotted column';
SELECT plain, plain.null FROM t_plain_null_wide;

DROP TABLE t_dotted_null_wide;
DROP TABLE t_dotted_null_compact;
DROP TABLE t_dotted_null_no_share;
DROP TABLE t_plain_null_wide;
