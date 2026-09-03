select sumKahan(dummy) from remote('127.{2,3}', system.one);
select sumWithOverflow(dummy) from remote('127.{2,3}', system.one);
select sum(dummy) from remote('127.{2,3}', system.one);
