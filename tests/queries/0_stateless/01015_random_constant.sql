select randConstant() >= 0;
select randConstant() % 10 < 10;
select uniqExact(x) from (select randConstant() as x);
