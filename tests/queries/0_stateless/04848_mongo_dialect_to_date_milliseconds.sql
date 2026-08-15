-- Mongo's `$toDate` reads a numeric argument as Unix milliseconds: `1546300800000` is
-- 2019-01-01, not the year 2282 a seconds reading would produce. A string still parses as a
-- date. The literal and the column paths dispatch differently, so both are covered, and the
-- string column exercises the branch the type dispatch keeps for a non-numeric argument.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect = 'clickhouse';

DROP TABLE IF EXISTS to_date;
CREATE TABLE to_date (ms Int64, f Float64, s String) ENGINE = Memory;
INSERT INTO to_date VALUES (1546300800000, 1546300800500.0, '2019-01-01 00:00:00');

SET allow_experimental_mongo_dialect = 1;
SET dialect = 'mongo';

db.to_date.aggregate([{"$project" : {"literal" : {"$toDate" : 1546300800000}, "negative" : {"$toDate" : -86400000}, "fractional" : {"$toDate" : 1546300800500.5}, "intColumn" : {"$toDate" : "$ms"}, "floatColumn" : {"$toDate" : "$f"}, "stringLiteral" : {"$toDate" : "2019-01-01 00:00:00"}, "stringColumn" : {"$toDate" : "$s"}}}]);

db.to_date.find({"$expr" : {"$eq" : [{"$toDate" : "$ms"}, {"$toDate" : "2019-01-01 00:00:00"}]}});

SET dialect = 'clickhouse';
DROP TABLE to_date;
