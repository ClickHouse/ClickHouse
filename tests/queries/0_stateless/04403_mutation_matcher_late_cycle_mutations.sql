-- Matcher expansion in stored expressions is settings-independent, so mutations that
-- re-evaluate a matcher-based MATERIALIZED column (`MATERIALIZE COLUMN`, `CLEAR COLUMN`)
-- produce the same result regardless of `asterisk_include_*` session settings.
DROP TABLE IF EXISTS mutation_matcher_materialize;
DROP TABLE IF EXISTS mutation_matcher_clear;

SET asterisk_include_materialized_columns = 0;

CREATE TABLE mutation_matcher_materialize
(
    a UInt8,
    m String MATERIALIZED toJSONString(tuple(*))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO mutation_matcher_materialize (a) VALUES (1);

SET asterisk_include_materialized_columns = 1;

-- `*` never includes the MATERIALIZED column itself, so no cycle forms even with
-- `asterisk_include_materialized_columns = 1`.
ALTER TABLE mutation_matcher_materialize MATERIALIZE COLUMN m SETTINGS mutations_sync = 1;
SELECT a, m FROM mutation_matcher_materialize;

SET asterisk_include_materialized_columns = 0;

CREATE TABLE mutation_matcher_clear
(
    a UInt8 DEFAULT 1,
    m String MATERIALIZED toJSONString(tuple(*))
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO mutation_matcher_clear (a) VALUES (2);

SET asterisk_include_materialized_columns = 1;

-- The dependent MATERIALIZED column `m` is rebuilt from the physically cleared value of `a`
-- (zero), while `a` itself reads its DEFAULT; the point here is that the mutation succeeds
-- and its result does not depend on the session settings.
ALTER TABLE mutation_matcher_clear CLEAR COLUMN a SETTINGS mutations_sync = 1;
SELECT a, m FROM mutation_matcher_clear;

DROP TABLE mutation_matcher_clear;
DROP TABLE mutation_matcher_materialize;
