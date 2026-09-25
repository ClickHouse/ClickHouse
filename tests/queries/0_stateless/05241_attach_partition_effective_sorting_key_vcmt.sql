-- Tags: no-replicated-database
-- ^ ATTACH with an explicit partition id; replicated DB has its own attach path,
-- and this test targets the non-replicated one.

-- ATTACH PARTITION FROM used to validate only the declared ORDER BY
-- (getSortingKeyAST), not the effective sorting key. For
-- VersionedCollapsingMergeTree the version column is appended to the sorting key
-- implicitly (ORDER BY (date, key) becomes (date, key, version)), so a staging
-- plain MergeTree whose declared ORDER BY matches the target was accepted even
-- though its parts are not sorted by (date, key, version). The version collapsing
-- then silently produced wrong results: 100 sign=1 rows + 100 sign=-1 rows of the
-- same key and version must collapse to 0, but FINAL returned a non-zero count.
-- Now the ordering check compares effective sorting key columns, so such an ATTACH
-- is refused up front.

DROP TABLE IF EXISTS vcmt;
DROP TABLE IF EXISTS staging_mt;
DROP TABLE IF EXISTS staging_vcmt;

CREATE TABLE vcmt (date Date, key Int32, value Int32, sign Int8, version UInt64)
ENGINE = VersionedCollapsingMergeTree(sign, version) PARTITION BY date ORDER BY (date, key);

-- Staging plain MergeTree: declared ORDER BY matches, effective one does not.
CREATE TABLE staging_mt (date Date, key Int32, value Int32, sign Int8, version UInt64)
ENGINE = MergeTree() PARTITION BY date ORDER BY (date, key);

INSERT INTO staging_mt SELECT '1970-10-10', 1, number, 1, number FROM numbers(100);
INSERT INTO staging_mt SELECT '1970-10-10', 1, number, -1, number FROM numbers(100) ORDER BY number % 5;

-- Must be refused: effective sorting keys differ, (date, key) vs (date, key, version).
ALTER TABLE vcmt ATTACH PARTITION '1970-10-10' FROM staging_mt; -- { serverError 36 }

-- Nothing was attached.
SELECT 'count after refused attach', count() FROM vcmt;

-- Supported path: staging with the same engine. VCMT parts are written sorted by the
-- effective key (version is appended), so ATTACH is safe and collapsing is correct.
CREATE TABLE staging_vcmt (date Date, key Int32, value Int32, sign Int8, version UInt64)
ENGINE = VersionedCollapsingMergeTree(sign, version) PARTITION BY date ORDER BY (date, key);

INSERT INTO staging_vcmt SELECT '1970-10-10', 1, number, 1, number FROM numbers(100);
INSERT INTO staging_vcmt SELECT '1970-10-10', 1, number, -1, number FROM numbers(100) ORDER BY number % 5;

ALTER TABLE vcmt ATTACH PARTITION '1970-10-10' FROM staging_vcmt;

SELECT 'count after vcmt staging attach', count() FROM vcmt;
SELECT 'count FINAL after vcmt staging attach', count() FROM vcmt FINAL;

-- The invariant is one-way: adopted parts only need to be sorted by the destination's
-- effective key. A staging whose effective key is STRONGER (here VCMT appends the
-- version column, giving (date, key, version)) stays valid for a plain MergeTree dest
-- whose key (date, key) is a prefix of it.
CREATE TABLE dst_mt (date Date, key Int32, value Int32)
ENGINE = MergeTree PARTITION BY date ORDER BY (date, key);

ALTER TABLE dst_mt ATTACH PARTITION '1970-10-10' FROM staging_vcmt;

SELECT 'count after stronger staging attach', count() FROM dst_mt;

DROP TABLE vcmt;
DROP TABLE staging_mt;
DROP TABLE staging_vcmt;
DROP TABLE dst_mt;