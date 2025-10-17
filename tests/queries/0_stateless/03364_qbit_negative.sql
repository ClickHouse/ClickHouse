SET allow_experimental_qbit_type = 1;

DROP TABLE IF EXISTS qbits;

CREATE TABLE qbits (id UInt32, vec QBit(BFloat16, 0)) ENGINE = Memory; -- { serverError UNEXPECTED_AST_STRUCTURE }
CREATE TABLE qbits (id UInt32, vec QBit(UInt32, 2)) ENGINE = Memory; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

CREATE TABLE qbits (id UInt32, vec QBit(Float64, 1)) ENGINE = Memory;
INSERT INTO qbits VALUES (1, array(1.0)::QBit(Float64, 1));
INSERT INTO qbits VALUES (1, array(1)); -- { error TYPE_MISMATCH }
SELECT vec::QBit(Float32, 1) FROM qbits; -- { serverError NOT_IMPLEMENTED }
SELECT vec::QBit(Float64, 2) FROM qbits; -- { serverError TYPE_MISMATCH }
SELECT vec::QBit(Float64, 1) FROM qbits;
DROP TABLE qbits;
