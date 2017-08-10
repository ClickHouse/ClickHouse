with pow(2,2) as four select pow(four, 2), 2 as two, pow(two, 2);
select `pow(four, 2)`, `pow(2, 2)` from (with pow(2,2) as four select pow(four, 2), 2 as two, pow(two, 2));
with (select pow(2,2)) as four select pow(four, 2), 2 as two, pow(two, 2);
select `pow(four, 2)`, `pow(2, 2)` from (with (select pow(2,2)) as four select pow(four, 2), 2 as two, pow(two, 2));
with 'string' as str select str || '_abc';
select `concat(str, \'_abc\')` from (with 'string' as str select str || '_abc');


