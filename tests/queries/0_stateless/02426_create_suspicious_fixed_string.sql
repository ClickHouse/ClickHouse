CREATE TABLE fixed_string (id UInt64, s FixedString(256)) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE suspicious_fixed_string (id UInt64, s FixedString(257)) ENGINE = MergeTree() ORDER BY id; -- { serverError ILLEGAL_COLUMN }
SET allow_suspicious_fixed_string_types = 1;
CREATE TABLE suspicious_fixed_string (id UInt64, s FixedString(257)) ENGINE = MergeTree() ORDER BY id;
