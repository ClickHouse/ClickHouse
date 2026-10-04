-- Regression test for the plain `CREATE TABLE ... AS SELECT` temporary-table publish path (issue #26746):
-- `CREATE TABLE IF NOT EXISTS ... AS SELECT` over an existing table must be a no-op even when the (new,
-- unused) definition would fail create-only validation, such as the cyclic table dependency check.
-- The existence fast path must run before those validations, mirroring `doCreateTable`.

DROP TABLE IF EXISTS dst;
DROP DICTIONARY IF EXISTS dict_on_dst;

CREATE TABLE dst (x UInt8, v UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO dst VALUES (1, 42);

-- The dictionary reads from `dst`, so it depends on `dst`.
CREATE DICTIONARY dict_on_dst (x UInt8, v UInt8) PRIMARY KEY x
SOURCE(CLICKHOUSE(TABLE 'dst'))
LAYOUT(FLAT())
LIFETIME(0);

-- A definition whose DEFAULT expression depends on `dict_on_dst` would close the cycle
-- dst -> dict_on_dst -> dst, so actually creating this table must be rejected. But with
-- IF NOT EXISTS on an existing table it must be a no-op that never reaches the cycle check.
SELECT 'if_not_exists_noop';
CREATE TABLE IF NOT EXISTS dst (x UInt8, v UInt8 DEFAULT dictGet('dict_on_dst', 'v', x)) ENGINE = MergeTree ORDER BY x
AS SELECT 2, 43;

-- The no-op must leave the existing table untouched: original definition, original data.
SELECT count(), sum(x), sum(v) FROM dst;
SELECT 'has_default_in_definition', countSubstrings(create_table_query, 'DEFAULT') FROM system.tables WHERE database = currentDatabase() AND name = 'dst';

-- Without IF NOT EXISTS the same statement must fail fast with TABLE_ALREADY_EXISTS
-- (from the same fast path, again before the cycle check).
SELECT 'without_if_not_exists';
CREATE TABLE dst (x UInt8, v UInt8 DEFAULT dictGet('dict_on_dst', 'v', x)) ENGINE = MergeTree ORDER BY x
AS SELECT 2, 43; -- { serverError TABLE_ALREADY_EXISTS }

-- The reordering must not lose the validation: when the table does not exist yet, a cyclic
-- definition is still rejected, and the failed create leaves no orphan table behind.
CREATE DICTIONARY dict_on_missing (x UInt8, v UInt8) PRIMARY KEY x
SOURCE(CLICKHOUSE(TABLE 'missing'))
LAYOUT(FLAT())
LIFETIME(0);

SELECT 'cyclic_create_still_rejected';
CREATE TABLE missing (x UInt8, v UInt8 DEFAULT dictGet('dict_on_missing', 'v', x)) ENGINE = MergeTree ORDER BY x
AS SELECT 2, 43; -- { serverError INFINITE_LOOP }
SELECT 'no_orphan', count() FROM system.tables WHERE database = currentDatabase() AND name = 'missing';

DROP DICTIONARY dict_on_missing;
DROP DICTIONARY dict_on_dst;
DROP TABLE dst;
