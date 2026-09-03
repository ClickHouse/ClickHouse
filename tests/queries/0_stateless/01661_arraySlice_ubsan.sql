-- { echo }
-- tests with INT64_MIN (UBsan)
select arraySlice([], -9223372036854775808);
