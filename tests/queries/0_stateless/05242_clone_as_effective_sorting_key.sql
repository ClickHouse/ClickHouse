-- Tags: no-replicated-database
-- ^ `CREATE TABLE ... CLONE AS ... ENGINE = VersionedCollapsingMergeTree` is not supported with
-- Replicated databases (`CREATE ... CLONE AS` as a whole is).

-- A `CREATE ... CLONE AS` propagates the source sorting key, so a target engine that appends a column to the
-- effective sorting key (like `VersionedCollapsingMergeTree(sign, version)`) ends up with parts sorted weaker
-- than the key. The internal `REPLACE PARTITION ALL FROM` adopted during the clone refuses such parts, but it
-- used to run only after the destination table was already published, leaving an orphan empty table behind.
-- The effective-sorting-key check must run before publication: the clone is rejected with the same error and
-- no table is left.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS src_vcmt;
DROP TABLE IF EXISTS dst_refused;
DROP TABLE IF EXISTS dst_ok;
DROP TABLE IF EXISTS dst_weak;

CREATE TABLE src (date Date, key Int32, value Int32, sign Int8, version UInt64)
ENGINE = MergeTree PARTITION BY date ORDER BY (date, key);
INSERT INTO src SELECT '1970-10-10', 1, number, number % 2 ? 1 : -1, number FROM numbers(100);

CREATE TABLE src_vcmt (date Date, key Int32, value Int32, sign Int8, version UInt64)
ENGINE = VersionedCollapsingMergeTree(sign, version) PARTITION BY date ORDER BY (date, key);
INSERT INTO src_vcmt SELECT '1970-10-10', 1, number, number % 2 ? 1 : -1, number FROM numbers(100);

-- Target appends `version` to the effective sorting key: parts from src are weaker, so the clone is refused.
CREATE TABLE dst_refused CLONE AS src ENGINE = VersionedCollapsingMergeTree(sign, version); -- { serverError 36 }

-- No orphan table was left behind, so a retry does not hit `TABLE_ALREADY_EXISTS`.
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'dst_refused';

-- Same-engine clone keeps working.
CREATE TABLE dst_ok CLONE AS src;
SELECT count() FROM dst_ok;

-- A VCMT source cloned into a plain MergeTree target is still allowed: weaker target key is a prefix of the source key.
CREATE TABLE dst_weak CLONE AS src_vcmt ENGINE = MergeTree;
SELECT count() FROM dst_weak;

DROP TABLE src;
DROP TABLE src_vcmt;
DROP TABLE dst_ok;
DROP TABLE dst_weak;