DROP TABLE IF EXISTS Bin_at_test;
CREATE TABLE Bin_at_test
(    
    `Date` DateTime('UTC'),
    Num Nullable(UInt8)
) ENGINE = Memory;
INSERT INTO Bin_at_test VALUES ('2018-02-24T15:14:01',3), ('2018-02-23T16:14:01',4), ('2018-02-26T15:14:01',5);

set dialect = 'kusto';
print '-- bin_at()';
print bin_at(6.5, 2.5, 7);
print bin_at(1h, 1d, 12h);
print bin_at(datetime(2017-05-15 10:20:00.0), 1d, datetime(1970-01-01 12:00:00.0));
print bin_at(datetime(2017-05-17 10:20:00.0), 7d, datetime(2017-06-04 00:00:00.0));
Bin_at_test | summarize sum(Num) by d = todatetime(bin_at(Date, 1d, datetime('2018-02-24 15:14:00'))) | order by d;
print '-- bin()';
print bin(4.5, 1);
print bin(datetime(1970-05-11 13:45:07), 1d);
print bin(16d, 7d);
print bin(datetime(1970-05-11 13:45:07.345623), 1ms);
-- print bin(datetime(2022-09-26 10:13:23.987234), 6ms); -> 2022-09-26 10:13:23.982000000
print bin(datetime(1970-05-11 13:45:07.345623), 1microsecond);
print bin(datetime(2022-09-26 10:13:23.987234), 6microseconds);
print bin(datetime(1970-05-11 13:45:07.456345672), 16microseconds);
-- print bin(datetime(2022-09-26 10:13:23.987234128), 1tick); -> 2022-09-26 10:13:23.987234100
-- print bin(datetime(2022-09-26 10:13:23.987234128), 99nanosecond); -> null
