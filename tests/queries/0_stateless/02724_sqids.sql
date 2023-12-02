SET allow_experimental_hash_functions = 1;

select sqids(1,2,3);
select number, sqids(number, number+1, number+2) from system.numbers limit 5;
