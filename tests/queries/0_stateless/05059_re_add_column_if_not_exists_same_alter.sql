-- Regression: a same-statement `DROP COLUMN x, ADD COLUMN IF NOT EXISTS x ...` must re-add the
-- column. `AlterCommands::prepare` judged the ADD's `IF NOT EXISTS` against the untouched original
-- schema (where x is still present), marked the ADD as a no-op, and the DROP silently removed x for
-- good. When x was the only column the table was left with an empty column list, so a later ALTER
-- raised "Cannot alter table ... metadata doesn't have structure".

DROP TABLE IF EXISTS re_add_ine;
CREATE TABLE re_add_ine (a Int64, x Int64, pad Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO re_add_ine VALUES (1, 10, 100), (2, 20, 200);

-- Dropping x and re-adding it (with IF NOT EXISTS) in one statement must leave x present, holding
-- the new default. Other columns keep their rows.
ALTER TABLE re_add_ine (DROP COLUMN x), (ADD COLUMN IF NOT EXISTS x Int64 DEFAULT 7);
SELECT 're-add count', count(), min(a), max(a) FROM re_add_ine;
SELECT 're-add x', groupArray(x) FROM re_add_ine;

-- The table must still have a usable column list (subsequent reads and ALTERs work).
SELECT 'read after', a, x FROM re_add_ine ORDER BY a;

-- A duplicate ADD on a column that still exists (not dropped in this statement) is still a no-op.
ALTER TABLE re_add_ine ADD COLUMN IF NOT EXISTS x Int64 DEFAULT 99;
SELECT 'dup-noop count', count() FROM system.columns
    WHERE database = currentDatabase() AND table = 're_add_ine' AND name = 'x';

DROP TABLE re_add_ine;
