SELECT if(number % 2, 9223372036854775806, -9223372036854775808) AS res FROM numbers(2);
SELECT if(number % 2, materialize(9223372036854775806), -9223372036854775808) AS res FROM numbers(2);
SELECT if(number % 2, 9223372036854775806, materialize(-9223372036854775808)) AS res FROM numbers(2);
SELECT if(number % 2, materialize(9223372036854775806), materialize(-9223372036854775808)) AS res FROM numbers(2);
SELECT if(number % 2, [9223372036854775806], [2, 65537, -9223372036854775808]) AS res FROM numbers(2);
SELECT if(number % 2, materialize([9223372036854775806]), [2, 65537, -9223372036854775808]) AS res FROM numbers(2);
SELECT if(number % 2, [9223372036854775806], materialize([2, 65537, -9223372036854775808])) AS res FROM numbers(2);
SELECT if(number % 2, materialize([9223372036854775806]), materialize([2, 65537, -9223372036854775808])) AS res FROM numbers(2);
SELECT if(number % 2, [[9223372036854775806]], [[2, 65537, -9223372036854775808]]) AS res FROM numbers(2);
SELECT if(number % 2, materialize([[9223372036854775806]]), [[2, 65537, -9223372036854775808]]) AS res FROM numbers(2);
SELECT if(number % 2, [[9223372036854775806]], materialize([[2, 65537, -9223372036854775808]])) AS res FROM numbers(2);
SELECT if(number % 2, materialize([[9223372036854775806]]), materialize([[2, 65537, -9223372036854775808]])) AS res FROM numbers(2);

