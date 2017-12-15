SELECT toDate(s) FROM (SELECT arrayJoin(['2017-01-02', '2017-1-02', '2017-01-2', '2017-1-2', '2017/01/02', '2017/1/02', '2017/01/2', '2017/1/2', '2017-11-12']) AS s);

DROP TABLE IF EXISTS test.date;
CREATE TABLE test.date (d Date) ENGINE = Memory;

INSERT INTO test.date VALUES ('2017-01-02'), ('2017-1-02'), ('2017-01-2'), ('2017-1-2'), ('2017/01/02'), ('2017/1/02'), ('2017/01/2'), ('2017/1/2'), ('2017-11-12');
SELECT * FROM test.date;

INSERT INTO test.date FORMAT JSONEachRow {"d": "2017-01-02"}, {"d": "2017-1-02"}, {"d": "2017-01-2"}, {"d": "2017-1-2"}, {"d": "2017/01/02"}, {"d": "2017/1/02"}, {"d": "2017/01/2"}, {"d": "2017/1/2"}, {"d": "2017-11-12"};
SELECT * FROM test.date ORDER BY d;

DROP TABLE test.date;

WITH toDate('2000-01-01') + rand() % (30000) AS EventDate SELECT * FROM numbers(1000000) WHERE EventDate != toDate(concat(toString(toYear(EventDate)), '-', toString(toMonth(EventDate)), '-', toString(toDayOfMonth(EventDate))));
