select sum(number) from remote('127.0.0.{2,3}', numbers(2)) where number global in (select sum(number) from numbers(2) group by number with totals) group by number with totals order by number;
