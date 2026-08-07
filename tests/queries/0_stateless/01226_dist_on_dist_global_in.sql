-- Tags: global

SELECT 'GLOBAL IN';
select * from remote('localhost', system.one) where dummy global in (0);
select * from remote('localhost', system.one) where dummy global in system.one;
select * from remote('localhost', system.one) where dummy global in (select 0);
SELECT 'GLOBAL NOT IN';
select * from remote('localhost', system.one) where dummy global not in (0);
select * from remote('localhost', system.one) where dummy global not in system.one;
select * from remote('localhost', system.one) where dummy global not in (select 0);
