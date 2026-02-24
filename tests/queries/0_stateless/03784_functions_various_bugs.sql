-- Some bugs found by gtest_functions_stress_test.cpp

select crc32(toFixedString(toString(number), 1)) from numbers(2);
select trimRight('', '01234567890123456'); -- { serverError TOO_LARGE_STRING_SIZE }
select kql_array_sort_asc([]::Array(Float32), number::UInt8) from numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select randomFixedString(CAST(607668569663131286404589520 AS UInt128)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select IPv6NumToString(IPv6StringToNumOrDefault(materialize(CAST('1:0:0:0::' AS FixedString(10))))) from numbers(2);
select IPv6NumToString(IPv6StringToNumOrDefault(materialize(CAST('1:0:0:0::\0' AS String)))) from numbers(2);
select IPv6NumToString(IPv6StringToNumOrDefault(x)) from system.one array join ['1::','oh no'] as x;
select reverseUTF8('\xfe'); -- { serverError UNICODE_ERROR }
select IPv6NumToString(IPv6StringToNumOrDefault(x)) from system.one array join ['24', '5.123.234'] as x;
select intDiv(1, materialize(CAST(null as LowCardinality(Nullable(Int8))))) settings allow_suspicious_low_cardinality_types=1;
select -1::Int32 as x, 2::Int32 as y, x%y, materialize(x)%y;
select indexOfAssumeSorted(cast([445696906, 73819614, 4294965975, 4294967294] as Array(IPv4)), 1268352529617758::UInt128); -- { serverError BAD_TYPE_OF_FIELD }
select caseWithExpression(0, 0, 1::Int128, 2);
