DROP TABLE IF EXISTS default_matcher_apply_lambda_scope;

CREATE TABLE default_matcher_apply_lambda_scope
(
    a UInt8,
    x UInt8,
    b UInt8 DEFAULT tupleElement(tuple(COLUMNS('^a$') APPLY (x -> x)), 1),
    c UInt8 DEFAULT tupleElement(tuple(COLUMNS('^a$') APPLY (x -> arrayElement(arrayMap(x -> a + x, [10]), 1))), 1)
)
ENGINE = Memory;

INSERT INTO default_matcher_apply_lambda_scope (a, x) VALUES (5, 9);

SELECT b, c FROM default_matcher_apply_lambda_scope;

DROP TABLE default_matcher_apply_lambda_scope;
