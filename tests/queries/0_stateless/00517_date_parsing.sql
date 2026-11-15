SELECT toDate(s) FROM (SELECT arrayJoin(['2017-01-02', '2017-1-02', '2017-01-2', '2017-1-2', '2017/01/02', '2017/1/02', '2017/01/2', '2017/1/2', '2017-11-12']) AS s);

DROP TABLE IF EXISTS date;
CREATE TABLE date (d Date) ENGINE = Memory;

INSERT INTO date VALUES ('2017-01-02'), ('2017-1-02'), ('2017-01-2'), ('2017-1-2'), ('2017/01/02'), ('2017/1/02'), ('2017/01/2'), ('2017/1/2'), ('2017-11-12');
SELECT * FROM date;

INSERT INTO date FORMAT JSONEachRow {"d": "2017-01-02"}, {"d": "2017-1-02"}, {"d": "2017-01-2"}, {"d": "2017-1-2"}, {"d": "2017/01/02"}, {"d": "2017/1/02"}, {"d": "2017/01/2"}, {"d": "2017/1/2"}, {"d": "2017-11-12"};

SELECT * FROM date ORDER BY d;

DROP TABLE date;

WITH toDate('2000-01-01') + rand() % (30000) AS EventDate SELECT * FROM numbers(1000000) WHERE EventDate != toDate(concat(toString(toYear(EventDate)), '-', toString(toMonth(EventDate)), '-', toString(toDayOfMonth(EventDate))));
