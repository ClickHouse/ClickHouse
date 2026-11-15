-- indexOfAssumeSorted with incompatible types in constant array should throw an exception, not crash (std::terminate from noexcept).
-- https://github.com/ClickHouse/ClickHouse/issues/92975
SELECT indexOfAssumeSorted(['1.1.1.1'::IPv4], 0); -- { serverError BAD_TYPE_OF_FIELD }
SELECT indexOfAssumeSorted(['172.181.59.225'::IPv4], 3350671033650519441::Int8); -- { serverError BAD_TYPE_OF_FIELD }
