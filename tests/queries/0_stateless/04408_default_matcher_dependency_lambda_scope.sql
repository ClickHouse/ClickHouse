DROP TABLE IF EXISTS default_matcher_dependency_lambda_scope;

CREATE TABLE default_matcher_dependency_lambda_scope
(
    z UInt8
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO default_matcher_dependency_lambda_scope VALUES (0);

ALTER TABLE default_matcher_dependency_lambda_scope ADD COLUMN x UInt8 ALIAS toUInt8(arrayElement(arrayMap(a -> a + 1, [1]), 1));
ALTER TABLE default_matcher_dependency_lambda_scope ADD COLUMN b UInt8 DEFAULT toUInt8(COLUMNS('^x$'));
ALTER TABLE default_matcher_dependency_lambda_scope ADD COLUMN a UInt8 DEFAULT b;

SELECT a, b FROM default_matcher_dependency_lambda_scope;

DROP TABLE default_matcher_dependency_lambda_scope;
