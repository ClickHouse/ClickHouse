DROP TABLE IF EXISTS default_expression_lambda_cycle;

CREATE TABLE default_expression_lambda_cycle
(
    x UInt8,
    y Array(UInt8) DEFAULT arrayMap(x -> x + 1, [1])
)
ENGINE = Memory;

ALTER TABLE default_expression_lambda_cycle MODIFY COLUMN x UInt8 DEFAULT length(y);

INSERT INTO default_expression_lambda_cycle (y) VALUES ([2]);

SELECT x, y FROM default_expression_lambda_cycle;

DROP TABLE default_expression_lambda_cycle;
