-- Tags: no-fasttest

select number, hashid(number) from system.numbers limit 5;
select number, hashid(number, 's3cr3t', 16, 'abcdefghijklmnop') from system.numbers limit 5;
select hashid(1234567890123456, 's3cr3t');
select hashid(-1);
