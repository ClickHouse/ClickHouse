-- The naming scheme is recorded per part, so one table can hold parts written with either one. Every
-- read side path has to take it from the part rather than from the current table setting.

DROP TABLE IF EXISTS t_mixed;

-- CI randomizes hashing and sparse serialization, and both change the file set, so they are pinned
-- wherever file names are asserted below.
CREATE TABLE t_mixed (id UInt64, s Nullable(String), a Array(UInt64), n Nested(b UInt64, c UInt64))
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'basic',
             replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_mixed VALUES (1, 'ab', [1,2], [10,20], [30,40]);
ALTER TABLE t_mixed MODIFY SETTING substream_naming_version = 'namespaced';
INSERT INTO t_mixed VALUES (2, NULL, [3], [50], [60]);

SELECT 'two parts, two schemes';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_mixed' AND active;

SELECT 'read across both parts';
SELECT id, s, a, n.b, n.c FROM t_mixed ORDER BY id;
SELECT id, s.null, a.size0, n.b.size0 FROM t_mixed ORDER BY id;

-- Introspection has to follow the part, not the current table setting, which by now differs from
-- what the first part holds.
SELECT 'names reported per part';
SELECT name, arraySort(groupArrayDistinct(f)) FROM
    (SELECT name, arrayJoin(filenames) AS f FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 't_mixed' AND active AND column = 'a')
    GROUP BY name ORDER BY name;

SELECT 'reload from metadata';
DETACH TABLE t_mixed;
ATTACH TABLE t_mixed;
SELECT id, s, a, n.b, n.c FROM t_mixed ORDER BY id;

SELECT 'merge the two schemes into one part';
OPTIMIZE TABLE t_mixed FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_mixed' AND active;
SELECT id, s, a, n.b, n.c FROM t_mixed ORDER BY id;

SELECT 'mutate';
ALTER TABLE t_mixed UPDATE a = arrayMap(x -> x + 1, a) WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT id, a FROM t_mixed ORDER BY id;

SELECT 'rename a column';
ALTER TABLE t_mixed RENAME COLUMN s TO s2;
SELECT id, s2, s2.null FROM t_mixed ORDER BY id;

DROP TABLE t_mixed;

-- With everything else default the info version drops to the oldest that fits, leaving the scheme as
-- the only thing recorded. Reattaching reloads it from disk, which is where it has to survive.
DROP TABLE IF EXISTS t_only_naming;
CREATE TABLE t_only_naming (a Array(Nullable(String))) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'namespaced';
INSERT INTO t_only_naming VALUES (['ab',NULL,'cde']);

SELECT 'naming version survives a reload on its own';
DETACH TABLE t_only_naming;
ATTACH TABLE t_only_naming;
SELECT a, a.size0, a.null FROM t_only_naming;
DROP TABLE t_only_naming;

-- Switching back is allowed too, so a table can end up with the old scheme after the new one.
DROP TABLE IF EXISTS t_back;
CREATE TABLE t_back (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'namespaced';
INSERT INTO t_back VALUES ([1,2]);
ALTER TABLE t_back MODIFY SETTING substream_naming_version = 'basic';
INSERT INTO t_back VALUES ([3]);

SELECT 'switched back';
SELECT a, a.size0 FROM t_back ORDER BY a;
OPTIMIZE TABLE t_back FINAL;
SELECT a, a.size0 FROM t_back ORDER BY a;
DROP TABLE t_back;

-- The scheme is persisted on its own, so it survives an info version that would otherwise leave the
-- part with no metadata file at all. Unrecorded, the part resolves the old names: that loses the
-- offsets stream of `Array(Nullable)` and makes a JSON path read the stream it is named after.
DROP TABLE IF EXISTS t_no_info;
CREATE TABLE t_no_info (x Array(Nullable(UInt64)), c JSON(`object_structure` Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             substream_naming_version = 'namespaced', serialization_info_version = 'basic';
INSERT INTO t_no_info VALUES ([1,NULL,3], '{"object_structure":5}');

SELECT 'scheme is independent of the info version';
DETACH TABLE t_no_info;
ATTACH TABLE t_no_info;
SELECT x, x.size0, x.null FROM t_no_info;
SELECT c.object_structure FROM t_no_info;
-- The streams really are namespaced, so the scheme was honoured and not quietly downgraded. Only
-- their presence is asserted: the JSON shared data contributes a randomized number of substreams.
SELECT countIf(f LIKE '%arr_elems%') > 0, countIf(f LIKE '%object_paths%') > 0 FROM
    (SELECT arrayJoin(filenames) AS f FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 't_no_info' AND active);
DROP TABLE t_no_info;

-- Both knobs move independently: two parts under one "basic" info version, one per scheme.
DROP TABLE IF EXISTS t_orthogonal;
CREATE TABLE t_orthogonal (x Array(Nullable(UInt64))) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             substream_naming_version = 'basic', serialization_info_version = 'basic',
             replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_orthogonal VALUES ([1,NULL,3]);
ALTER TABLE t_orthogonal MODIFY SETTING substream_naming_version = 'namespaced';
INSERT INTO t_orthogonal VALUES ([7]);

SELECT 'both schemes under a basic info version';
SELECT x FROM t_orthogonal ORDER BY x;
SELECT name, countIf(f LIKE '%arr_elems%') FROM
    (SELECT name, arrayJoin(filenames) AS f FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 't_orthogonal' AND active)
    GROUP BY name ORDER BY name;
OPTIMIZE TABLE t_orthogonal FINAL;
SELECT x FROM t_orthogonal ORDER BY x;
DROP TABLE t_orthogonal;
