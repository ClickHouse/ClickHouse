SELECT
    substringIndex('a.b.c', '.', toUInt64(9223372036854775807)),
    substringIndexUTF8('a。b。c', '。', toUInt64(9223372036854775807));

SELECT substringIndex('a.b.c', '.', toUInt64(9223372036854775808)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringIndex(materialize('a.b.c'), '.', toUInt64(18446744073709551615)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringIndex('a.b.c', '.', materialize(toUInt64(9223372036854775808))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringIndex(materialize('a.b.c'), '.', materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT substringIndexUTF8('a。b。c', '。', toUInt64(9223372036854775808)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringIndexUTF8(materialize('a。b。c'), '。', toUInt64(18446744073709551615)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringIndexUTF8('a。b。c', '。', materialize(toUInt64(9223372036854775808))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringIndexUTF8(materialize('a。b。c'), '。', materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
