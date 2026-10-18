SET enable_named_columns_in_function_tuple = 1;

DROP TABLE IF EXISTS alter_default_expr_matchers_add_alias;
DROP TABLE IF EXISTS alter_default_expr_matchers_modify_alias;
DROP TABLE IF EXISTS alter_default_expr_matchers_add_materialized;
DROP TABLE IF EXISTS alter_default_expr_matchers_modify_materialized;

CREATE TABLE alter_default_expr_matchers_add_alias
(
    a UInt8
)
ENGINE = Memory;

ALTER TABLE alter_default_expr_matchers_add_alias ADD COLUMN b String ALIAS toJSONString(tuple(*));
INSERT INTO alter_default_expr_matchers_add_alias (a) VALUES (1);
SELECT b FROM alter_default_expr_matchers_add_alias;

CREATE TABLE alter_default_expr_matchers_modify_alias
(
    a UInt8,
    b String
)
ENGINE = Memory;

ALTER TABLE alter_default_expr_matchers_modify_alias MODIFY COLUMN b String ALIAS toJSONString(tuple(*));
INSERT INTO alter_default_expr_matchers_modify_alias (a) VALUES (1);
SELECT b FROM alter_default_expr_matchers_modify_alias;

CREATE TABLE alter_default_expr_matchers_add_materialized
(
    a UInt8
)
ENGINE = Memory;

ALTER TABLE alter_default_expr_matchers_add_materialized ADD COLUMN b String MATERIALIZED toJSONString(tuple(*));
INSERT INTO alter_default_expr_matchers_add_materialized (a) VALUES (1);
SELECT b FROM alter_default_expr_matchers_add_materialized;

CREATE TABLE alter_default_expr_matchers_modify_materialized
(
    a UInt8,
    b String
)
ENGINE = Memory;

ALTER TABLE alter_default_expr_matchers_modify_materialized MODIFY COLUMN b String MATERIALIZED toJSONString(tuple(*));
INSERT INTO alter_default_expr_matchers_modify_materialized (a) VALUES (1);
SELECT b FROM alter_default_expr_matchers_modify_materialized;

DROP TABLE alter_default_expr_matchers_modify_materialized;
DROP TABLE alter_default_expr_matchers_add_materialized;
DROP TABLE alter_default_expr_matchers_modify_alias;
DROP TABLE alter_default_expr_matchers_add_alias;
