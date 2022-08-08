SET allow_experimental_hash_functions = 1;

select number, hashid(number) from system.numbers limit 5;
select number, hashid(number, 's3cr3t', 16, 'abcdefghijklmnop') from system.numbers limit 5;
select hashid(1234567890123456, 's3cr3t');
select hashid(1234567890123456, 's3cr3t2');

SELECT  hashid(1, hashid(2));
SELECT  hashid(1, 'k5');
SELECT  hashid(1, 'k5_othersalt');

-- https://github.com/ClickHouse/ClickHouse/issues/39672
SELECT
    JSONExtractRaw(257, NULL),
    hashid(1024, if(rand() % 10, 'truetruetruetrue', NULL), 's3\0r3t'); -- {serverError 43}
