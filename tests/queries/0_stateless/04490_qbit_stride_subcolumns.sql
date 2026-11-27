-- Positional subcolumns of a strided QBit map to (stride group, bit plane) in group-major order:
-- q.N reads bit plane (N-1) % element_size of stride group (N-1) / element_size.

SELECT 'QBit(BFloat16, 16, 8): q.1 .. q.16 -> first stride, q.17 .. q.32 -> second stride';
DROP TABLE IF EXISTS qbit_stride;
CREATE TABLE qbit_stride (id UInt32, vec QBit(BFloat16, 16, 8)) ENGINE = Memory;
-- First stride group (dims 1..8) is all -0.0 (sign bit set), second group (dims 9..16) is all +0.0 (sign bit clear).
INSERT INTO qbit_stride VALUES (1, [-0, -0, -0, -0, -0, -0, -0, -0, 0, 0, 0, 0, 0, 0, 0, 0]);

SELECT 'sign plane of group 0 (q.1)', bin(vec.1) FROM qbit_stride;
SELECT 'sign plane of group 1 (q.17)', bin(vec.17) FROM qbit_stride;
SELECT 'subcolumn type', toTypeName(vec.1) FROM qbit_stride;

SELECT vec.0 FROM qbit_stride; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT vec.33 FROM qbit_stride; -- { serverError ARGUMENT_OUT_OF_BOUND }

DROP TABLE qbit_stride;
