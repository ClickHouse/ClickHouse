DROP TABLE IF EXISTS constraint_constant_number_expression;
CREATE TABLE constraint_constant_number_expression
(
    id UInt64,
    CONSTRAINT `c0` CHECK 1,
    CONSTRAINT `c1` CHECK 1 < 2,
    CONSTRAINT `c2` CHECK isNull(cast(NULL, 'Nullable(UInt8)'))
) ENGINE = TinyLog();

INSERT INTO constraint_constant_number_expression VALUES (1);

SELECT * FROM constraint_constant_number_expression;

DROP TABLE constraint_constant_number_expression;

DROP TABLE IF EXISTS constraint_constant_number_expression_non_uint8;
CREATE TABLE constraint_constant_number_expression_non_uint8
(
    id UInt64,
    CONSTRAINT `c0` CHECK toUInt64(1)
) ENGINE = TinyLog();

INSERT INTO constraint_constant_number_expression_non_uint8 VALUES (1); -- {serverError 1}

SELECT * FROM constraint_constant_number_expression_non_uint8;

DROP TABLE constraint_constant_number_expression_non_uint8;
