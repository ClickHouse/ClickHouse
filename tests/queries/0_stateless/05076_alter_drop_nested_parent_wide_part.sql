-- Regression: the wide-part mutation fast path (hardlinking untouched columns) ignored a
-- `DROP COLUMN` that targets a `Nested` parent, because the part stores the nested range
-- flattened (`n.x`, `n.y`, ...) and no stored column is literally named `n`. The files of the
-- dropped members were hardlinked into the new part, and a later `ADD COLUMN IF NOT EXISTS n.x`
-- made the stale data readable again (see 05059_re_add_column_if_not_exists_same_alter, which
-- only caught this under randomized `min_bytes_for_wide_part`). The table settings below pin the
-- wide-part layout so the fast path is exercised deterministically.

DROP TABLE IF EXISTS drop_nested_wide;

CREATE TABLE drop_nested_wide (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO drop_nested_wide VALUES (1, [10], [20]), (2, [11], [21]);

SELECT 'part type', part_type FROM system.parts
    WHERE database = currentDatabase() AND table = 'drop_nested_wide' AND active;

-- Dropping the parent must remove the whole flattened range from the new part.
ALTER TABLE drop_nested_wide DROP COLUMN n;
SELECT 'after drop parent', a FROM drop_nested_wide ORDER BY a;
SELECT 'columns after drop parent', name FROM system.columns
    WHERE database = currentDatabase() AND table = 'drop_nested_wide' ORDER BY name;
CHECK TABLE drop_nested_wide;
DROP TABLE drop_nested_wide;

-- A same-name re-add must not resurrect the dropped data: the re-added column reads defaults.
CREATE TABLE readd_nested_wide (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO readd_nested_wide VALUES (1, [10], [20]), (2, [11], [21]);
ALTER TABLE readd_nested_wide (DROP COLUMN n), (ADD COLUMN IF NOT EXISTS `n.x` Array(Int64));
SELECT 're-add columns', name FROM system.columns
    WHERE database = currentDatabase() AND table = 'readd_nested_wide' ORDER BY name;
SELECT 're-add data', a, n.x FROM readd_nested_wide ORDER BY a;
CHECK TABLE readd_nested_wide;
DROP TABLE readd_nested_wide;

-- CLEAR COLUMN of the parent must reset all members to defaults while other columns keep data.
CREATE TABLE clear_nested_wide (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO clear_nested_wide VALUES (1, [10], [20]), (2, [11], [21]);
ALTER TABLE clear_nested_wide CLEAR COLUMN n;
SELECT 'after clear parent', a, n.x, n.y FROM clear_nested_wide ORDER BY a;
CHECK TABLE clear_nested_wide;
DROP TABLE clear_nested_wide;
