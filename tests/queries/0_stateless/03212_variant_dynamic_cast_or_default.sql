set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;
set allow_experimental_dynamic_type = 1;

select accurateCastOrDefault(variant, 'UInt32'), multiIf(number % 4 == 0, NULL, number % 4 == 1, number, number % 4 == 2, 'str_' || toString(number), range(number)) as variant from numbers(8);
select accurateCastOrNull(variant, 'UInt32'), multiIf(number % 4 == 0, NULL, number % 4 == 1, number, number % 4 == 2, 'str_' || toString(number), range(number)) as variant from numbers(8);

select accurateCastOrDefault(dynamic, 'UInt32'), multiIf(number % 4 == 0, NULL, number % 4 == 1, number, number % 4 == 2, 'str_' || toString(number), range(number))::Dynamic as dynamic from numbers(8);
select accurateCastOrNull(dynamic, 'UInt32'), multiIf(number % 4 == 0, NULL, number % 4 == 1, number, number % 4 == 2, 'str_' || toString(number), range(number))::Dynamic as dynamic from numbers(8);
