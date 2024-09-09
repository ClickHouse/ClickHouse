set use_variant_as_common_type = 1;
set allow_experimental_variant_type = 1;

SELECT if(number % 2, tuple(number), tuple(toString(number))) as res, toTypeName(res) FROM numbers(5);
SELECT if(number % 2, map(number, number), map(toString(number), toString(number))) as res, toTypeName(res) FROM numbers(5);


