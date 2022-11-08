-- { echoOn }
select toDate(1666249120::Float);
select toDate(1666249120::Double);
select toDate(1666249120::UInt32);

select toDate32(1666249120::Float);
select toDate32(1666249120::Double);
select toDate32(1666249120::UInt32);

select toDateTime(1666249120::Float);
select toDateTime(1666249120::Double);
select toDateTime(1666249120::UInt32);

select toDateTime64(1666249120::Float, 3);
select toDateTime64(1666249120::Double, 3);
select toDateTime64(1666249120::UInt32, 3);
-- { echoOff }
