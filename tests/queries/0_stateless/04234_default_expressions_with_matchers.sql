SET enable_named_columns_in_function_tuple = 1;

DROP TABLE IF EXISTS default_expr_matchers_alias;
DROP TABLE IF EXISTS default_expr_matchers_alias_apply;
DROP TABLE IF EXISTS default_expr_matchers_alias_columns;
DROP TABLE IF EXISTS default_expr_matchers_alias_columns_list;
DROP TABLE IF EXISTS default_expr_matchers_alias_nested_matcher;
DROP TABLE IF EXISTS default_expr_matchers_alias_replace;
DROP TABLE IF EXISTS default_expr_matchers_alias_tuple;
DROP TABLE IF EXISTS default_expr_matchers_default_except;
DROP TABLE IF EXISTS default_expr_matchers_ephemeral_asterisk;
DROP TABLE IF EXISTS default_expr_matchers_ephemeral_columns;
DROP TABLE IF EXISTS default_expr_matchers_ephemeral_not_matched;
DROP TABLE IF EXISTS default_expr_matchers_include_alias;
DROP TABLE IF EXISTS default_expr_matchers_include_materialized;
DROP TABLE IF EXISTS default_expr_matchers_index_one;
DROP TABLE IF EXISTS default_expr_matchers_index_tuple;

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

CREATE TABLE default_expr_matchers_alias_columns_list
(
    a UInt8,
    c String,
    d UInt8,
    b String ALIAS toJSONString(tuple(COLUMNS(a, c)))
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_alias_columns_list (a, c, d) VALUES (1, 'x', 2);
SELECT b FROM default_expr_matchers_alias_columns_list;

CREATE TABLE default_expr_matchers_alias_apply
(
    a UInt8,
    c String,
    b String ALIAS toJSONString(tuple(* APPLY toString))
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_alias_apply (a, c) VALUES (1, 'x');
SELECT b FROM default_expr_matchers_alias_apply;

CREATE TABLE default_expr_matchers_alias_replace
(
    a UInt8,
    c String,
    b String ALIAS toJSONString(tuple(* REPLACE (2 AS a)))
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_alias_replace (a, c) VALUES (1, 'x');
SELECT b FROM default_expr_matchers_alias_replace;

CREATE TABLE default_expr_matchers_alias_nested_matcher
(
    a UInt8,
    c String,
    b String ALIAS toJSONString(tuple(* REPLACE (array(COLUMNS(a, c) APPLY toString) AS a)))
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_alias_nested_matcher (a, c) VALUES (1, 'x');
SELECT b FROM default_expr_matchers_alias_nested_matcher;

CREATE TABLE default_expr_matchers_invalid_regexp
(
    a UInt8,
    b String ALIAS toJSONString(tuple(COLUMNS('(')))
)
ENGINE = Memory; -- { serverError CANNOT_COMPILE_REGEXP }

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

CREATE TABLE default_expr_matchers_indirect_self
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(* EXCEPT b)),
    c String DEFAULT b
)
ENGINE = Memory; -- { serverError CYCLIC_ALIASES }

SET asterisk_include_materialized_columns = 1;
CREATE TABLE default_expr_matchers_materialized_self
(
    a UInt8,
    b String MATERIALIZED toJSONString(tuple(*))
)
ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
SET asterisk_include_materialized_columns = 0;

SET asterisk_include_alias_columns = 1;
CREATE TABLE default_expr_matchers_alias_self
(
    a UInt8,
    b String ALIAS toJSONString(tuple(*))
)
ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
SET asterisk_include_alias_columns = 0;

CREATE TABLE default_expr_matchers_default_except
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(* EXCEPT b))
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_default_except (a) VALUES (1);
SELECT b FROM default_expr_matchers_default_except;

CREATE TABLE default_expr_matchers_include_materialized
(
    a UInt8,
    m UInt8 MATERIALIZED a + 1,
    b String DEFAULT toJSONString(tuple(* EXCEPT b))
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
    b String DEFAULT toJSONString(tuple(* EXCEPT b))
)
ENGINE = Memory;

SET asterisk_include_alias_columns = 1;
INSERT INTO default_expr_matchers_include_alias (a) VALUES (1);
SET asterisk_include_alias_columns = 0;
SELECT b FROM default_expr_matchers_include_alias;

CREATE TABLE default_expr_matchers_qualified_asterisk_error
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(t.*))
)
ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

CREATE TABLE default_expr_matchers_qualified_columns_error
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(t.COLUMNS('^a$')))
)
ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

CREATE TABLE default_expr_matchers_qualified_columns_list_error
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(t.COLUMNS(a)))
)
ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

CREATE TABLE default_expr_matchers_ephemeral_asterisk
(
    a UInt8,
    b String EPHEMERAL toJSONString(tuple(*)),
    c String MATERIALIZED b
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_ephemeral_asterisk (a) VALUES (1);
SELECT c FROM default_expr_matchers_ephemeral_asterisk;

CREATE TABLE default_expr_matchers_ephemeral_columns
(
    a UInt8,
    c String,
    d UInt8,
    b String EPHEMERAL toJSONString(tuple(COLUMNS('^(a|c)$'))),
    e String MATERIALIZED b
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_ephemeral_columns (a, c, d) VALUES (1, 'x', 2);
SELECT e FROM default_expr_matchers_ephemeral_columns;

CREATE TABLE default_expr_matchers_ephemeral_not_matched
(
    a UInt8,
    b UInt8 EPHEMERAL a + 1,
    c String DEFAULT toJSONString(tuple(COLUMNS('^(a|b)$')))
)
ENGINE = Memory;

INSERT INTO default_expr_matchers_ephemeral_not_matched (a) VALUES (1);
SELECT c FROM default_expr_matchers_ephemeral_not_matched;

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
DROP TABLE default_expr_matchers_ephemeral_not_matched;
DROP TABLE default_expr_matchers_ephemeral_columns;
DROP TABLE default_expr_matchers_ephemeral_asterisk;
DROP TABLE default_expr_matchers_include_alias;
DROP TABLE default_expr_matchers_include_materialized;
DROP TABLE default_expr_matchers_default_except;
DROP TABLE default_expr_matchers_alias_nested_matcher;
DROP TABLE default_expr_matchers_alias_replace;
DROP TABLE default_expr_matchers_alias_apply;
DROP TABLE default_expr_matchers_alias_columns_list;
DROP TABLE default_expr_matchers_alias_columns;
DROP TABLE default_expr_matchers_alias_tuple;
DROP TABLE default_expr_matchers_alias;
