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
