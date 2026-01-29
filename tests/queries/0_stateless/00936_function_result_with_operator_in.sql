SET force_primary_key = 1;

DROP TABLE IF EXISTS samples;
CREATE TABLE samples (key UInt32, value UInt32) ENGINE = MergeTree() ORDER BY key PRIMARY KEY key;
INSERT INTO samples VALUES (1, 1)(2, 2)(3, 3)(4, 4)(5, 5);

-- all etries, verify that index is used
SELECT count() FROM samples WHERE key IN range(10);

-- some entries:
SELECT count() FROM samples WHERE key IN arraySlice(range(100), 5, 10);

-- different type
SELECT count() FROM samples WHERE toUInt64(key) IN range(100);

SELECT 'empty:';
-- should be empty
SELECT count() FROM samples WHERE key IN arraySlice(range(100), 10, 10);

-- not only ints:
SELECT 'a' IN splitByChar('c', 'abcdef');

SELECT 'errors:';
-- non-constant expressions in the right side of IN
SELECT count() FROM samples WHERE 1 IN range(samples.value); -- { serverError UNSUPPORTED_METHOD, 47 }
SELECT count() FROM samples WHERE 1 IN range(rand() % 1000); -- { serverError UNSUPPORTED_METHOD, 36 }

-- index is not used
SELECT count() FROM samples WHERE value IN range(3); -- { serverError INDEX_NOT_USED }

-- wrong type
SELECT 123 IN splitByChar('c', 'abcdef'); -- { serverError TYPE_MISMATCH }

DROP TABLE samples;
