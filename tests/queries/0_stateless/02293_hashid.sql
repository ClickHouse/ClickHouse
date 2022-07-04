SET allow_experimental_hash_functions = 1;

select number, hashid(number) from system.numbers limit 5;
select number, hashid(number, 's3cr3t', 16, 'abcdefghijklmnop') from system.numbers limit 5;
select hashid(1234567890123456, 's3cr3t');

SELECT  hashid(1, hashid(2));
