SELECT 1 where (NOT (SELECT [69::UInt64][1])); -- { serverError UNKNOWN_IDENTIFIER }

SELECT 1 where (NOT (SELECT [[69::UInt64]][1])); -- { serverError UNKNOWN_IDENTIFIER }
