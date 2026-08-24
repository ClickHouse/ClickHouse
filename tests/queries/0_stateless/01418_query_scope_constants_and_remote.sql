select c >= 0 from (SELECT randConstant() as c FROM remote('127.0.0.{1,2}', numbers_mt(1)));
select c >= 0 from (SELECT randConstant() as c FROM remote('127.0.0.{3,2}', numbers_mt(1)));
select c >= 0 from (SELECT randConstant() as c FROM remote('127.0.0.1', numbers_mt(1)));