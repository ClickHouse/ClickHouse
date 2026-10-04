-- `$dateFromString` reads a date without an offset as UTC, as every other date of this dialect
-- does. The `format` argument only constrains the syntax of the text, so the formatted and the
-- best effort forms answer with the same instant even when the session is in another time zone.
-- The two dates are printed in UTC either way, so the equality is what tells them apart.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect = 'clickhouse';
SET session_timezone = 'Asia/Istanbul';

DROP TABLE IF EXISTS date_from_string;
CREATE TABLE date_from_string (_id Int64, s String) ENGINE = Memory;
INSERT INTO date_from_string VALUES (1, '2013-07-15');

SET allow_experimental_mongo_dialect = 1;
SET dialect = 'mongo';

db.date_from_string.aggregate([{"$project" : {"formatted" : {"$dateFromString" : {"dateString" : "$s", "format" : "%Y-%m-%d"}}, "bestEffort" : {"$dateFromString" : {"dateString" : "$s"}}, "sameInstant" : {"$eq" : [{"$dateFromString" : {"dateString" : "$s", "format" : "%Y-%m-%d"}}, {"$dateFromString" : {"dateString" : "$s"}}]}}}]);

SET dialect = 'clickhouse';
DROP TABLE date_from_string;
