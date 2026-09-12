-- Matcher `REPLACE (expr AS name)` in stored column expressions must validate under the
-- old analyzer too: the expansion produces a self-alias (`a + 1 AS a`), which used to be
-- rejected with CYCLIC_ALIASES by `TreeRewriter` during CREATE-time default validation.
SET enable_analyzer = 0;

DROP TABLE IF EXISTS default_expr_matcher_replace_legacy;
DROP TABLE IF EXISTS default_expr_matcher_replace_legacy_materialized;
DROP TABLE IF EXISTS default_expr_matcher_replace_legacy_self_alias;

-- Stored DEFAULT with `REPLACE` whose replacement expression references the replaced column.
CREATE TABLE default_expr_matcher_replace_legacy
(
    a UInt32,
    b UInt32,
    s String DEFAULT toJSONString(tuple(COLUMNS('^(a|b)$') REPLACE (a + 1 AS a)))
)
ENGINE = Memory;

INSERT INTO default_expr_matcher_replace_legacy (a, b) VALUES (1, 2);
SELECT s FROM default_expr_matcher_replace_legacy;

-- The same for MATERIALIZED.
CREATE TABLE default_expr_matcher_replace_legacy_materialized
(
    a UInt32,
    b UInt32,
    s String MATERIALIZED toJSONString(tuple(COLUMNS('^(a|b)$') REPLACE (a + 1 AS a)))
)
ENGINE = Memory;

INSERT INTO default_expr_matcher_replace_legacy_materialized (a, b) VALUES (3, 4);
SELECT s FROM default_expr_matcher_replace_legacy_materialized;

-- An explicit inner self-alias without matchers behaves the same as on the analyzer path.
CREATE TABLE default_expr_matcher_replace_legacy_self_alias
(
    a UInt32,
    s String DEFAULT toString(a + 1 AS a)
)
ENGINE = Memory;

INSERT INTO default_expr_matcher_replace_legacy_self_alias (a) VALUES (5);
SELECT s FROM default_expr_matcher_replace_legacy_self_alias;

-- Genuine cyclic defaults are still rejected under the old analyzer.
CREATE TABLE default_expr_matcher_replace_legacy_cycle (a UInt32 DEFAULT a + 1) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
CREATE TABLE default_expr_matcher_replace_legacy_cycle (a UInt32, c String DEFAULT toJSONString(tuple(* REPLACE (a + 1 AS a)))) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }

DROP TABLE default_expr_matcher_replace_legacy;
DROP TABLE default_expr_matcher_replace_legacy_materialized;
DROP TABLE default_expr_matcher_replace_legacy_self_alias;
