-- https://github.com/ClickHouse/ClickHouse/issues/66307
-- Using parameterized view to obtain a scalar result

CREATE VIEW paramview AS
    SELECT * FROM system.numbers
    WHERE number <= {top:UInt64};

SELECT arrayReduce('sum', (SELECT groupArray(number) FROM paramview(top = 10)));

DROP VIEW paramview;
