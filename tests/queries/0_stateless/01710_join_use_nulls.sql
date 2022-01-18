DROP TABLE IF EXISTS X;
DROP TABLE IF EXISTS Y;

CREATE TABLE X (id Int) ENGINE=Memory;
CREATE TABLE Y (id Int) ENGINE=Memory;

-- Type mismatch of columns to JOIN by: plus(id, 1) Int64 at left, Y.id Int32 at right.
SELECT Y.id - 1 FROM X RIGHT JOIN Y ON (X.id + 1) = Y.id SETTINGS join_use_nulls=1; -- { serverError 53 }
SELECT Y.id - 1 FROM X RIGHT JOIN Y ON (X.id + 1) = toInt64(Y.id) SETTINGS join_use_nulls=1;

-- Logical error: 'Arguments of 'plus' have incorrect data types: '2' of type 'UInt8', '1' of type 'UInt8''.
-- Because 1 became toNullable(1), i.e.:
--     2 UInt8 Const(size = 1, UInt8(size = 1))
--     1 UInt8 Const(size = 1, Nullable(size = 1, UInt8(size = 1), UInt8(size = 1)))
SELECT 2+1 FROM system.one X RIGHT JOIN system.one Y ON X.dummy+1 = Y.dummy SETTINGS join_use_nulls = 1; -- { serverError 53 }
SELECT 2+1 FROM system.one X RIGHT JOIN system.one Y ON X.dummy+1 = toUInt16(Y.dummy) SETTINGS join_use_nulls = 1;
SELECT X.dummy+1 FROM system.one X RIGHT JOIN system.one Y ON X.dummy = Y.dummy SETTINGS join_use_nulls = 1;
SELECT Y.dummy+1 FROM system.one X RIGHT JOIN system.one Y ON X.dummy = Y.dummy SETTINGS join_use_nulls = 1;

DROP TABLE X;
DROP TABLE Y;
