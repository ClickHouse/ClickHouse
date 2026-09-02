set use_variant_as_common_type = 1;
set allow_experimental_variant_type = 1;

SELECT if(number % 2, number::Int64, number::UInt64) as res, toTypeName(res) FROM numbers(2);
SELECT if(number % 2, number::Int32, number::UInt64) as res, toTypeName(res) FROM numbers(2);
SELECT if(number % 2, number::Int16, number::UInt64) as res, toTypeName(res) FROM numbers(2);
SELECT if(number % 2, number::Int8, number::UInt64) as res, toTypeName(res) FROM numbers(2);

