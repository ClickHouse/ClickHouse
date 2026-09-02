select intDiv(materialize(toInt32(1)), 0x100000000);
select intDiv(materialize(toInt32(1)), -0x100000000);
select intDiv(materialize(toInt32(1)), -9223372036854775808);
select materialize(toInt32(1)) % -9223372036854775808;
select value % -9223372036854775808 from (select toInt32(arrayJoin([3, 5])) value);
