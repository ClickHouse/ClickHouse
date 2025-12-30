--- GitHub issue 56144 
--- Tests number values of enum are validated during insertion.

SET input_format_numbers_enum_on_conversion_error = 0; -- default value

DROP TABLE IF EXISTS enum_table;
CREATE TABLE enum_table (
  id UInt64,
  e Enum('first' = 1, 'second' = 2, 'third' = 3)
) ENGINE = Memory;

INSERT INTO enum_table VALUES (0, 'first');
INSERT INTO enum_table VALUES (0, 'fifth'); -- { clientError UNKNOWN_ELEMENT_OF_ENUM }
INSERT INTO enum_table VALUES (0, 0);

SET input_format_numbers_enum_on_conversion_error = 1;

INSERT INTO enum_table VALUES (0, 'first');
INSERT INTO enum_table VALUES (0, 'fifth'); -- { clientError UNKNOWN_ELEMENT_OF_ENUM }
INSERT INTO enum_table VALUES (0, 0); -- { clientError UNKNOWN_ELEMENT_OF_ENUM }

DROP TABLE enum_table;

SET input_format_numbers_enum_on_conversion_error = 1;

SELECT (('first'::String)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT (('second'::String)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT (('third'::String)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT (('fifth'::String)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

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

SELECT ((4::Int8)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT ((4::Int16)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT ((4::Int32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT ((4::Int64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT ((4::UInt8)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT ((4::UInt16)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT ((4::UInt32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT ((4::UInt64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT ((4::Float32)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT ((4::Float64)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

SET input_format_numbers_enum_on_conversion_error = 0;

SELECT (('first'::String)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT (('second'::String)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT (('third'::String)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64;
SELECT (('fifth'::String)::Enum('first' = 1, 'second' = 2, 'third' = 3))::UInt64; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

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
