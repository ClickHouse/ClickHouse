-- Tags: shard, no-old-analyzer
-- The `Distributed` rewrite of duplicate `ALIAS` columns names the wrapping
-- `__actionName` call `__aliasColumn_<source ordinal>_<column name>`. Such a name can also be
-- spelled out by the user, either as a physical column or as an alias of an expression of the
-- same query. This must not make the shard reuse one planner node for both, because column
-- action names in the plan are column identifiers (`__table1.__aliasColumn_0_d`) and user
-- aliases are not action names at all, so the namespaces never intersect.
-- The fix lives in the analyzer's distributed rewrite (`buildQueryTreeDistributed`), so the
-- test is restricted to the analyzer.

DROP TABLE IF EXISTS shard_alias_name_collision;
DROP TABLE IF EXISTS dist_alias_name_collision;

CREATE TABLE shard_alias_name_collision
(
    a String,
    b Float64,
    c Float64,
    `__aliasColumn_0_d` Float64,
    d Float64 ALIAS b + c,
    e Float64 ALIAS b + c
)
ENGINE = MergeTree() ORDER BY a;

INSERT INTO shard_alias_name_collision VALUES ('x', 1, 2, 777);

CREATE TABLE dist_alias_name_collision AS shard_alias_name_collision
ENGINE = Distributed(test_shard_localhost, currentDatabase(), shard_alias_name_collision);

-- A physical column named exactly like the generated action name of the wrapper for `d`.
SELECT `__aliasColumn_0_d`, d, e FROM dist_alias_name_collision;
SELECT d, e, `__aliasColumn_0_d` FROM dist_alias_name_collision;

-- The same name given to an unrelated expression by an explicit alias.
SELECT b + c + 5 AS `__aliasColumn_0_d`, d, e FROM dist_alias_name_collision;

-- An `ALIAS` column named like the generated name, next to the duplicates it could collide with.
DROP TABLE IF EXISTS shard_alias_named_like_wrapper;
DROP TABLE IF EXISTS dist_alias_named_like_wrapper;

CREATE TABLE shard_alias_named_like_wrapper
(
    a String,
    b Float64,
    c Float64,
    `__aliasColumn_0_d` Float64 ALIAS b + c,
    d Float64 ALIAS b + c,
    e Float64 ALIAS b + c
)
ENGINE = MergeTree() ORDER BY a;

INSERT INTO shard_alias_named_like_wrapper VALUES ('x', 1, 2);

CREATE TABLE dist_alias_named_like_wrapper AS shard_alias_named_like_wrapper
ENGINE = Distributed(test_shard_localhost, currentDatabase(), shard_alias_named_like_wrapper);

SELECT `__aliasColumn_0_d`, d, e FROM dist_alias_named_like_wrapper;
SELECT d, `__aliasColumn_0_d`, e FROM dist_alias_named_like_wrapper;

DROP TABLE dist_alias_named_like_wrapper;
DROP TABLE shard_alias_named_like_wrapper;
DROP TABLE dist_alias_name_collision;
DROP TABLE shard_alias_name_collision;
