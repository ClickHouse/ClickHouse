-- Tags: no-parallel, no-ordinary-database
-- Tag no-parallel, no-ordinary-database: the grandfathering case below needs an Ordinary database,
-- because only there can a table be ATTACHed from an explicit definition.

-- A text index over an ALIAS column whose declared type differs from the type its expression
-- produces can never be used: reading the column applies an implicit CAST to the declared type,
-- while the index is built over the expression as written, and the two no longer match by name.
-- The index was silently built, merged and never read. Reject it instead.
-- https://github.com/ClickHouse/ClickHouse/issues/111643

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_mistyped_alias;
DROP TABLE IF EXISTS t_matching_alias;

-- `JSONExtractKeys` returns `Array(String)`, so declaring `paths` as `String` is the mismatch.
CREATE TABLE t_mistyped_alias
(
    event String,
    paths String ALIAS JSONExtractKeys(event),
    INDEX fts_paths paths TYPE text(tokenizer = array)
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Declaring the column with the type its expression produces is accepted, and the index is used.
CREATE TABLE t_matching_alias
(
    event String,
    paths Array(String) ALIAS JSONExtractKeys(event),
    INDEX fts_paths paths TYPE text(tokenizer = array)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_matching_alias VALUES ('{"some":"value"}'), ('{"foo":"bar"}');

SELECT count() FROM t_matching_alias WHERE hasAllTokens(paths, ['xoo']);
SELECT count() FROM t_matching_alias WHERE hasAllTokens(paths, ['foo']);

-- Retyping the ALIAS column introduces the same mismatch without touching the index declaration,
-- so it is rejected too. Exempting an index merely because its own definition is unchanged would
-- let this through.
ALTER TABLE t_matching_alias MODIFY COLUMN paths String ALIAS JSONExtractKeys(event); -- { serverError BAD_ARGUMENTS }

-- An ALIAS column that is not indexed keeps converting as before.
DROP TABLE IF EXISTS t_alias_no_index;
CREATE TABLE t_alias_no_index
(
    event String,
    paths String ALIAS JSONExtractKeys(event)
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_no_index VALUES ('{"foo":"bar"}');
SELECT paths FROM t_alias_no_index;

DROP TABLE t_matching_alias;
DROP TABLE t_alias_no_index;

-- A table that already carries such an index keeps loading, and unrelated ALTERs on it still work.
-- `checkProperties` also runs for every ALTER and on the replica side of a committed ALTER_METADATA,
-- both with attach = false, so re-validating an index the operation does not touch would leave such
-- a table loadable but un-alterable - and would wedge the replication queue of an upgraded replica.
SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS db_05076_ord;
CREATE DATABASE db_05076_ord ENGINE = Ordinary;

ATTACH TABLE db_05076_ord.t_grandfathered
(
    event String,
    paths String ALIAS JSONExtractKeys(event),
    INDEX fts_paths paths TYPE text(tokenizer = array)
) ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE db_05076_ord.t_grandfathered ADD COLUMN x UInt8;
SELECT count() FROM system.columns WHERE database = 'db_05076_ord' AND table = 't_grandfathered' AND name = 'x';

-- Inheriting the violation is not a licence to rewrite it. Retyping the alias, redirecting it at a
-- different expression, or replacing the index under the same name all introduce a fresh unusable
-- index, so the name of the index and the name of the offending column are not enough to decide
-- that a violation was inherited - the definitions behind both have to be unchanged.
ALTER TABLE db_05076_ord.t_grandfathered
    MODIFY COLUMN paths String ALIAS splitByChar(',', event); -- { serverError BAD_ARGUMENTS }

ALTER TABLE db_05076_ord.t_grandfathered
    DROP INDEX fts_paths, ADD INDEX fts_paths paths TYPE text(tokenizer = splitByNonAlpha); -- { serverError BAD_ARGUMENTS }

-- The mismatch belongs to the ALIAS column, not to the shape of the index expression, so a wrapper
-- over a mistyped ALIAS is rejected too. `mapValues(a)` is indexed as `mapValues(m)` while a query
-- asks for `mapValues(_CAST(m, 'Map(String, FixedString(3))'))`, so the names never meet.
DROP TABLE IF EXISTS t_wrapped_alias;
CREATE TABLE t_wrapped_alias
(
    m Map(String, String),
    a Map(String, FixedString(3)) ALIAS m,
    INDEX fts_wrapped mapValues(a) TYPE text(tokenizer = array)
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Retyping a column the ALIAS is written in terms of changes what the index resolves to - here
-- Array(String) becomes Array(FixedString(2)) - while the index declaration, the ALIAS declaration
-- and the offending column name all stay put. That is a different unusable index, so inheriting the
-- old one is no licence for it.
DROP DATABASE IF EXISTS db_05076_dep;
CREATE DATABASE db_05076_dep ENGINE = Ordinary;

ATTACH TABLE db_05076_dep.t_dep
(
    m Map(String, String),
    a Array(FixedString(3)) ALIAS mapValues(m),
    INDEX fts_dep a TYPE text(tokenizer = array)
) ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE db_05076_dep.t_dep ADD COLUMN y UInt8;
SELECT count() FROM system.columns WHERE database = 'db_05076_dep' AND table = 't_dep' AND name = 'y';

ALTER TABLE db_05076_dep.t_dep
    MODIFY COLUMN m Map(String, FixedString(2)); -- { serverError BAD_ARGUMENTS }

DROP DATABASE db_05076_dep;

-- The escape hatch stays open: the index can still be dropped.
ALTER TABLE db_05076_ord.t_grandfathered DROP INDEX fts_paths;
SELECT count() FROM system.data_skipping_indices WHERE database = 'db_05076_ord' AND table = 't_grandfathered';

DROP DATABASE db_05076_ord;

-- A lambda parameter is bound by the expression, not a reference to a table column, so an index
-- expression whose lambda happens to name a column is not an index over that column.
DROP TABLE IF EXISTS t_lambda_shadow;
CREATE TABLE t_lambda_shadow
(
    event String,
    x String ALIAS JSONExtractKeys(event),
    arr Array(String),
    INDEX fts_lambda arrayMap(x -> lower(x), arr) TYPE text(tokenizer = array)
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lambda_shadow (event, arr) VALUES ('{}', ['Foo']);
SELECT count() FROM t_lambda_shadow WHERE hasAllTokens(arrayMap(x -> lower(x), arr), ['foo']);
DROP TABLE t_lambda_shadow;

-- The index is built over the ALIAS chain fully expanded, so a mistyped ALIAS anywhere under it
-- leaves the index unreachable even when the one named in the index is typed consistently.
CREATE TABLE t_chained_alias
(
    event String,
    a String ALIAS JSONExtractKeys(event),
    b String ALIAS toJSONString(a),
    INDEX fts_chained b TYPE text(tokenizer = splitByNonAlpha)
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- The index is built over its expression with every ALIAS expanded, so rewriting a link in that
-- chain rebuilds it over something else - even when the link itself is typed consistently and the
-- mistyped ALIAS beneath it is untouched.
SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS db_05076_chain;
CREATE DATABASE db_05076_chain ENGINE = Ordinary;

ATTACH TABLE db_05076_chain.t_chain
(
    event String,
    a String ALIAS JSONExtractKeys(event),
    b String ALIAS toJSONString(a),
    INDEX fts_chain b TYPE text(tokenizer = splitByNonAlpha)
) ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE db_05076_chain.t_chain ADD COLUMN z UInt8;
SELECT count() FROM system.columns WHERE database = 'db_05076_chain' AND table = 't_chain' AND name = 'z';

ALTER TABLE db_05076_chain.t_chain
    MODIFY COLUMN b String ALIAS toString(a); -- { serverError BAD_ARGUMENTS }

DROP DATABASE db_05076_chain;

-- The declared type of the offending ALIAS is the one input the comparisons above are blind to:
-- the expanded expression and the resolved types both come from the expression, so neither moves
-- when only the declaration is retyped. It is still a different unusable index.
SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS db_05076_declared;
CREATE DATABASE db_05076_declared ENGINE = Ordinary;

ATTACH TABLE db_05076_declared.t_declared
(
    m Map(String, String),
    a Array(FixedString(3)) ALIAS mapValues(m),
    INDEX fts_declared a TYPE text(tokenizer = array)
) ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE db_05076_declared.t_declared ADD COLUMN w UInt8;
SELECT count() FROM system.columns WHERE database = 'db_05076_declared' AND table = 't_declared' AND name = 'w';

ALTER TABLE db_05076_declared.t_declared
    MODIFY COLUMN a Array(FixedString(2)) ALIAS mapValues(m); -- { serverError BAD_ARGUMENTS }

-- Retyping it to the type the expression actually produces removes the violation, so it is allowed.
ALTER TABLE db_05076_declared.t_declared MODIFY COLUMN a Array(String) ALIAS mapValues(m);
SELECT count() FROM system.data_skipping_indices WHERE database = 'db_05076_declared' AND table = 't_declared';

DROP DATABASE db_05076_declared;

-- The type an ALIAS expression produces is the other half of what makes it mistyped, and an index
-- expression can normalise a change to it away: `lower()` maps `FixedString` back to `String`, so
-- retyping the column the ALIAS reads leaves the index declaration, the expanded expression, the
-- resolved types and the ALIAS's own declared type all equal while the CAST it is read through moves.
SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS db_05076_norm;
CREATE DATABASE db_05076_norm ENGINE = Ordinary;

ATTACH TABLE db_05076_norm.t_norm
(
    m Map(String, String),
    a Array(FixedString(3)) ALIAS mapValues(m),
    INDEX fts_norm arrayMap(x -> lower(x), a) TYPE text(tokenizer = array)
) ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE db_05076_norm.t_norm ADD COLUMN v UInt8;
SELECT count() FROM system.columns WHERE database = 'db_05076_norm' AND table = 't_norm' AND name = 'v';

ALTER TABLE db_05076_norm.t_norm
    MODIFY COLUMN m Map(String, FixedString(2)); -- { serverError BAD_ARGUMENTS }

DROP DATABASE db_05076_norm;
