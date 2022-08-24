DROP TABLE IF EXISTS Bin_at_test;
CREATE TABLE Bin_at_test
(    
    `Date` DateTime('UTC'),
    Num Nullable(UInt8)
) ENGINE = Memory;
INSERT INTO Bin_at_test VALUES (toDateTime('2018-02-24T15:14:00'),3), (toDateTime('2018-02-23T16:14:00'),4), (toDateTime('2018-02-26T15:14:00'),5);

set dialect = 'kusto';
print '-- bin_at()';
print bin_at(6.5, 2.5, 7);
print bin_at(1h, 1d, 12h);
print bin_at(datetime(2017-05-15 10:20:00.0), 1d, datetime(1970-01-01 12:00:00.0));
print bin_at(datetime(2017-05-17 10:20:00.0), 7d, datetime(2017-06-04 00:00:00.0));
Bin_at_test | summarize sum(Num) by toDateTime(bin_at(Date, 1d, datetime('2018-02-24 15:14:00')));
print '-- bin()';
print bin(4.5, 1);
print bin(datetime(1970-05-11 13:45:07), 1d);
print bin(16d, 7d);