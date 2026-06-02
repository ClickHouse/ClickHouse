DROP TABLE IF EXISTS default_expression_matcher_dependencies_read;
DROP TABLE IF EXISTS default_expression_matcher_dependencies_materialized;

CREATE TABLE default_expression_matcher_dependencies_read
(
    a UInt8,
    c UInt8
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO default_expression_matcher_dependencies_read VALUES (1, 3);

ALTER TABLE default_expression_matcher_dependencies_read ADD COLUMN b UInt8 DEFAULT plus(* EXCEPT b);

SELECT a, c, b FROM default_expression_matcher_dependencies_read;

DROP TABLE default_expression_matcher_dependencies_read;

SET apply_mutations_on_fly = 1;
SET mutations_sync = 0;

CREATE TABLE default_expression_matcher_dependencies_materialized
(
    id UInt64,
    a UInt8,
    c UInt8,
    m UInt8 MATERIALIZED greatest(* EXCEPT m)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES default_expression_matcher_dependencies_materialized;

INSERT INTO default_expression_matcher_dependencies_materialized (id, a, c) VALUES (1, 1, 3);

SELECT m FROM default_expression_matcher_dependencies_materialized;

ALTER TABLE default_expression_matcher_dependencies_materialized UPDATE a = 10 WHERE id = 1;

SELECT count()
FROM system.mutations
WHERE database = currentDatabase()
  AND table = 'default_expression_matcher_dependencies_materialized'
  AND NOT is_done;

SELECT m FROM default_expression_matcher_dependencies_materialized;
SELECT m FROM default_expression_matcher_dependencies_materialized SETTINGS apply_mutations_on_fly = 0;

SYSTEM START MERGES default_expression_matcher_dependencies_materialized;

DROP TABLE default_expression_matcher_dependencies_materialized;
