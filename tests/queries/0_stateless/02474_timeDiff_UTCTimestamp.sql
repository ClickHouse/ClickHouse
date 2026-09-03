-- all tests should be equal to zero as timediff is same as dateDiff('second', ... )
SELECT dateDiff('second', toDate32('1927-01-01'), toDate32('1927-01-02')) - timeDiff(toDate32('1927-01-01'), toDate32('1927-01-02')) <= 2;
SELECT dateDiff('second', toDate32('1927-01-01'), toDateTime64('1927-01-02 00:00:00', 3)) - timeDiff(toDate32('1927-01-01'), toDateTime64('1927-01-02 00:00:00', 3)) <= 2;
SELECT dateDiff('second', toDateTime64('1927-01-01 00:00:00', 3), toDate32('1927-01-02')) - timeDiff(toDateTime64('1927-01-01 00:00:00', 3), toDate32('1927-01-02')) <= 2;
SELECT dateDiff('second', toDate32('2015-08-18'), toDateTime('2015-08-19 00:00:00')) - timeDiff(toDate32('2015-08-18'), toDateTime('2015-08-19 00:00:00')) <= 2;
SELECT dateDiff('second', toDateTime('2015-08-18 00:00:00'), toDate32('2015-08-19')) - timeDiff(toDateTime('2015-08-18 00:00:00'), toDate32('2015-08-19')) <= 2;
SELECT dateDiff('second', toDate32('2015-08-18'), toDate('2015-08-19')) - timeDiff(toDate32('2015-08-18'), toDate('2015-08-19')) <= 2;
SELECT dateDiff('second', toDate('2015-08-18'), toDate32('2015-08-19')) - timeDiff(toDate('2015-08-18'), toDate32('2015-08-19')) <= 2;

-- UTCTimestamp equals to now('UTC')
SELECT dateDiff('s', UTCTimestamp(), now('UTC')) <= 2;
SELECT timeDiff(UTCTimestamp(), now('UTC')) <= 2;
