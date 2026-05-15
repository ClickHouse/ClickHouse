DROP TABLE IF EXISTS default_expr_matchers_alias;
DROP TABLE IF EXISTS default_expr_matchers_alias_columns;
DROP TABLE IF EXISTS default_expr_matchers_alias_tuple;
DROP TABLE IF EXISTS default_expr_matchers_default_except;
DROP TABLE IF EXISTS default_expr_matchers_index_one;
DROP TABLE IF EXISTS default_expr_matchers_index_tuple;

SET enable_named_columns_in_function_tuple = 1;

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
    b String ALIAS toJSONString(tuple(*))
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_alias_tuple (a, c) VALUES (1, 'x');
SELECT b FROM default_expr_matchers_alias_tuple;

CREATE TABLE default_expr_matchers_alias_columns
(
    a UInt8,
    c String,
    d UInt8,
    b String ALIAS toJSONString(tuple(COLUMNS('^(a|c)$')))
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
    b String DEFAULT toJSONString(tuple(*))
)
ENGINE = Memory; -- { serverError CYCLIC_ALIASES }

CREATE TABLE default_expr_matchers_default_except
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(* EXCEPT b))
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_default_except (a) VALUES (1);
SELECT b FROM default_expr_matchers_default_except;

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
    INDEX idx(toJSONString(tuple(*))) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

DROP TABLE default_expr_matchers_index_tuple;
DROP TABLE default_expr_matchers_index_one;
DROP TABLE default_expr_matchers_default_except;
DROP TABLE default_expr_matchers_alias_columns;
DROP TABLE default_expr_matchers_alias_tuple;
DROP TABLE default_expr_matchers_alias;
