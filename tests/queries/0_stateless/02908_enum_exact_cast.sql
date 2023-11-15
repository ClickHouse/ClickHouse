SET input_format_numbers_enum_on_conversion_error = 1;

SELECT ((2::Int8)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((2::Int16)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((2::Int32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((2::Int64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((2::UInt8)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((2::UInt16)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((2::UInt32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((2::UInt64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((2::Float32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((2::Float64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;

SELECT ((4::Int8)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError BAD_ARGUMENTS }
SELECT ((4::Int16)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError BAD_ARGUMENTS }
SELECT ((4::Int32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError BAD_ARGUMENTS }
SELECT ((4::Int64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError BAD_ARGUMENTS }
SELECT ((4::UInt8)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError BAD_ARGUMENTS }
SELECT ((4::UInt16)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError BAD_ARGUMENTS }
SELECT ((4::UInt32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError BAD_ARGUMENTS }
SELECT ((4::UInt64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError BAD_ARGUMENTS }
SELECT ((4::Float32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError BAD_ARGUMENTS }
SELECT ((4::Float64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError BAD_ARGUMENTS }

SET input_format_numbers_enum_on_conversion_error = 0;

SELECT ((4::Int8)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((4::Int16)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((4::Int32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((4::Int64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((4::UInt8)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((4::UInt16)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((4::UInt32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((4::UInt64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((4::Float32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT ((4::Float64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;

SET input_format_numbers_enum_on_conversion_error = 1;

DROP TABLE IF EXISTS enum_table;
CREATE TABLE enum_table (id UInt64, e Enum('first' = 1, 'second' = 2, 'third' = 3)) ENGINE = Memory;
INSERT INTO enum_table VALUES (0, 0); -- { clientError BAD_ARGUMENTS }

SET input_format_numbers_enum_on_conversion_error = 0;

INSERT INTO enum_table VALUES (0, 0);
DROP TABLE enum_table;
