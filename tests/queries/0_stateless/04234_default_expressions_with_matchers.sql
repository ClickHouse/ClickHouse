DROP TABLE IF EXISTS default_expr_matchers_alias;
DROP TABLE IF EXISTS default_expr_matchers_alias_columns;
DROP TABLE IF EXISTS default_expr_matchers_alias_tuple;
DROP TABLE IF EXISTS default_expr_matchers_default_except;
DROP TABLE IF EXISTS default_expr_matchers_include_alias;
DROP TABLE IF EXISTS default_expr_matchers_include_materialized;
DROP TABLE IF EXISTS default_expr_matchers_index_one;
DROP TABLE IF EXISTS default_expr_matchers_index_tuple;

SELECT toJSONString(namedTuple(1 AS a, 'x' AS c));
SELECT namedTuple(1, 2); -- { serverError BAD_ARGUMENTS }

CREATE TABLE default_expr_matchers_alias
(
    a UInt8,
    b String ALIAS toJSONString(*)
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_alias (a) VALUES (1);
SELECT b FROM default_expr_matchers_alias;

CREATE TABLE default_expr_matchers_alias_tuple
(
    a UInt8,
    c String,
    b String ALIAS toJSONString(namedTuple(*))
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_alias_tuple (a, c) VALUES (1, 'x');
SELECT b FROM default_expr_matchers_alias_tuple;

CREATE TABLE default_expr_matchers_alias_columns
(
    a UInt8,
    c String,
    d UInt8,
    b String ALIAS toJSONString(namedTuple(COLUMNS('^(a|c)$')))
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_alias_columns (a, c, d) VALUES (1, 'x', 2);
SELECT b FROM default_expr_matchers_alias_columns;

CREATE TABLE default_expr_matchers_error
(
    a UInt8,
    c UInt8,
    b String ALIAS toJSONString(*)
)
ENGINE = Memory; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

CREATE TABLE default_expr_matchers_self
(
    a UInt8,
    b String DEFAULT toJSONString(namedTuple(*))
)
ENGINE = Memory; -- { serverError CYCLIC_ALIASES }

CREATE TABLE default_expr_matchers_default_except
(
    a UInt8,
    b String DEFAULT toJSONString(namedTuple(* EXCEPT b))
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_default_except (a) VALUES (1);
SELECT b FROM default_expr_matchers_default_except;

CREATE TABLE default_expr_matchers_include_materialized
(
    a UInt8,
    m UInt8 MATERIALIZED a + 1,
    b String DEFAULT toJSONString(namedTuple(* EXCEPT b))
)
ENGINE = Memory;

SET asterisk_include_materialized_columns = 1;
INSERT INTO default_expr_matchers_include_materialized (a) VALUES (1);
SET asterisk_include_materialized_columns = 0;
SELECT b FROM default_expr_matchers_include_materialized;

CREATE TABLE default_expr_matchers_include_alias
(
    a UInt8,
    x UInt8 ALIAS a + 1,
    b String DEFAULT toJSONString(namedTuple(* EXCEPT b))
)
ENGINE = Memory;

SET asterisk_include_alias_columns = 1;
INSERT INTO default_expr_matchers_include_alias (a) VALUES (1);
SET asterisk_include_alias_columns = 0;
SELECT b FROM default_expr_matchers_include_alias;

CREATE TABLE default_expr_matchers_index_one
(
    a UInt8,
    INDEX idx(toJSONString(*)) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE default_expr_matchers_index_error
(
    a UInt8,
    b UInt8,
    INDEX idx(toJSONString(*)) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

CREATE TABLE default_expr_matchers_index_tuple
(
    a UInt8,
    b UInt8,
    INDEX idx(toJSONString(namedTuple(*))) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

DROP TABLE default_expr_matchers_index_tuple;
DROP TABLE default_expr_matchers_index_one;
DROP TABLE default_expr_matchers_include_alias;
DROP TABLE default_expr_matchers_include_materialized;
DROP TABLE default_expr_matchers_default_except;
DROP TABLE default_expr_matchers_alias_columns;
DROP TABLE default_expr_matchers_alias_tuple;
DROP TABLE default_expr_matchers_alias;
