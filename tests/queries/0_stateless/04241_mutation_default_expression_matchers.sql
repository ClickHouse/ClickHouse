DROP TABLE IF EXISTS mutation_default_expr_matchers_update;
DROP TABLE IF EXISTS mutation_default_expr_matchers_materialize_default;
DROP TABLE IF EXISTS mutation_default_expr_matchers_materialize;

CREATE TABLE mutation_default_expr_matchers_update
(
    a UInt8,
    c UInt8,
    m UInt8 MATERIALIZED greatest(*)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO mutation_default_expr_matchers_update (a, c) VALUES (1, 3);
SELECT a, c, m FROM mutation_default_expr_matchers_update;
ALTER TABLE mutation_default_expr_matchers_update UPDATE a = 5 WHERE c = 3 SETTINGS mutations_sync = 1;
SELECT a, c, m FROM mutation_default_expr_matchers_update;

CREATE TABLE mutation_default_expr_matchers_materialize_default
(
    a UInt8,
    c UInt8,
    d UInt8 DEFAULT greatest(* EXCEPT d)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO mutation_default_expr_matchers_materialize_default (a, c) VALUES (1, 3);
ALTER TABLE mutation_default_expr_matchers_materialize_default MATERIALIZE COLUMN d SETTINGS mutations_sync = 1;
SELECT a, c, d FROM mutation_default_expr_matchers_materialize_default;

CREATE TABLE mutation_default_expr_matchers_materialize
(
    a UInt8,
    c UInt8,
    m UInt8 MATERIALIZED 0
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO mutation_default_expr_matchers_materialize (a, c) VALUES (1, 3);
SELECT a, c, m FROM mutation_default_expr_matchers_materialize;
ALTER TABLE mutation_default_expr_matchers_materialize MODIFY COLUMN m UInt8 MATERIALIZED greatest(* EXCEPT m);
ALTER TABLE mutation_default_expr_matchers_materialize MATERIALIZE COLUMN m SETTINGS mutations_sync = 1;
SELECT a, c, m FROM mutation_default_expr_matchers_materialize;

DROP TABLE mutation_default_expr_matchers_materialize;
DROP TABLE mutation_default_expr_matchers_materialize_default;
DROP TABLE mutation_default_expr_matchers_update;
