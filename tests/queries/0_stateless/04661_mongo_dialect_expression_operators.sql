-- The aggregation expression operators, one group of them per query, following the categories
-- SingleStore Kai lists: arithmetic, string, array, set, date, conditional, comparison and type
-- conversion. Every group is projected out of a single row so that the expected value is written
-- next to the operator that produces it.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS one;
CREATE TABLE one (n Int32, m Int32, s String, t String, a Array(Int32), b Array(Int32), d DateTime) ENGINE = Memory;
INSERT INTO one VALUES (-7, 2, 'Hello World', 'o W', [3, 1, 2], [2, 4], '2013-07-15 10:20:30');

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';

db.one.aggregate([{"$project" : {"abs" : {"$abs" : "$n"}, "add" : {"$add" : ["$n", "$m", 1]}, "sub" : {"$subtract" : ["$n", "$m"]}, "mul" : {"$multiply" : ["$m", 3]}, "div" : {"$divide" : ["$n", "$m"]}, "mod" : {"$mod" : ["$m", 2]}, "pow" : {"$pow" : ["$m", 3]}, "ceil" : {"$ceil" : {"$divide" : ["$n", "$m"]}}, "floor" : {"$floor" : {"$divide" : ["$n", "$m"]}}, "trunc" : {"$trunc" : {"$divide" : ["$n", "$m"]}}, "log" : {"$log" : [8, 2]}}}]);

db.one.aggregate([{"$project" : {"concat" : {"$concat" : ["$s", "!"]}, "lenBytes" : {"$strLenBytes" : "$s"}, "lenCP" : {"$strLenCP" : "$s"}, "upper" : {"$toUpper" : "$t"}, "lower" : {"$toLower" : "$s"}, "indexOf" : {"$indexOfBytes" : ["$s", "$t"]}, "missing" : {"$indexOfBytes" : ["$s", "zzz"]}, "substr" : {"$substrBytes" : ["$s", 6, 5]}, "split" : {"$split" : ["$s", " "]}, "one" : {"$replaceOne" : {"input" : "$s", "find" : "l", "replacement" : "L"}}, "all" : {"$replaceAll" : {"input" : "$s", "find" : "l", "replacement" : "L"}}, "trim" : {"$trim" : {"input" : "  padded  "}}, "matches" : {"$regexMatch" : {"input" : "$s", "regex" : "^Hello"}}}}]);

db.one.aggregate([{"$project" : {"size" : {"$size" : "$a"}, "first" : {"$first" : "$a"}, "last" : {"$last" : "$a"}, "at" : {"$arrayElemAt" : ["$a", 1]}, "fromEnd" : {"$arrayElemAt" : ["$a", -1]}, "reversed" : {"$reverseArray" : "$a"}, "concat" : {"$concatArrays" : ["$a", "$b"]}, "indexOf" : {"$indexOfArray" : ["$a", 2]}, "in" : {"$in" : [2, "$a"]}, "range" : {"$range" : [0, 4, 2]}, "slice" : {"$slice" : ["$a", 2]}, "sliceEnd" : {"$slice" : ["$a", -2]}, "sliceFrom" : {"$slice" : ["$a", 1, 2]}, "mapped" : {"$map" : {"input" : "$a", "as" : "e", "in" : {"$multiply" : ["$$e", 10]}}}, "filtered" : {"$filter" : {"input" : "$a", "cond" : {"$gte" : ["$$this", 2]}}}}}]);

db.one.aggregate([{"$project" : {"union" : {"$setUnion" : ["$a", "$b"]}, "intersection" : {"$setIntersection" : ["$a", "$b"]}, "difference" : {"$setDifference" : ["$a", "$b"]}, "equals" : {"$setEquals" : ["$a", "$a"]}, "any" : {"$anyElementTrue" : [[0, 1]]}, "all" : {"$allElementsTrue" : [[0, 1]]}}}]);

db.one.aggregate([{"$project" : {"year" : {"$year" : "$d"}, "month" : {"$month" : "$d"}, "day" : {"$dayOfMonth" : "$d"}, "dayOfWeek" : {"$dayOfWeek" : "$d"}, "isoDayOfWeek" : {"$isoDayOfWeek" : "$d"}, "dayOfYear" : {"$dayOfYear" : "$d"}, "isoWeek" : {"$isoWeek" : "$d"}, "hour" : {"$hour" : "$d"}, "minute" : {"$minute" : "$d"}, "second" : {"$second" : "$d"}, "truncated" : {"$dateTrunc" : {"date" : "$d", "unit" : "hour"}}, "formatted" : {"$dateToString" : {"date" : "$d", "format" : "%Y/%m/%d"}}, "added" : {"$dateAdd" : {"startDate" : "$d", "unit" : "day", "amount" : 3}}, "subtracted" : {"$dateSubtract" : {"startDate" : "$d", "unit" : "hour", "amount" : 10}}, "difference" : {"$dateDiff" : {"startDate" : "$d", "endDate" : {"$date" : "2013-07-20T00:00:00Z"}, "unit" : "day"}}}}]);

db.one.aggregate([{"$project" : {"cond" : {"$cond" : {"if" : {"$lt" : ["$n", 0]}, "then" : "negative", "else" : "positive"}}, "condArray" : {"$cond" : [{"$gt" : ["$n", 0]}, 1, 0]}, "switch" : {"$switch" : {"branches" : [{"case" : {"$eq" : ["$m", 1]}, "then" : "one"}, {"case" : {"$eq" : ["$m", 2]}, "then" : "two"}], "default" : "many"}}, "ifNull" : {"$ifNull" : [null, "$s"]}, "cmpLess" : {"$cmp" : ["$n", "$m"]}, "cmpEqual" : {"$cmp" : ["$m", "$m"]}, "literal" : {"$literal" : 42}}}]);

db.one.aggregate([{"$project" : {"string" : {"$toString" : "$n"}, "int" : {"$toInt" : "3"}, "double" : {"$toDouble" : "1.5"}, "bool" : {"$toBool" : "$m"}, "decimal" : {"$toDecimal" : "$m"}, "date" : {"$toDate" : "2013-07-15 00:00:00"}}}]);

db.one.aggregate([{"$group" : {"_id" : null, "sum" : {"$sum" : "$n"}, "count" : {"$sum" : 1}, "avg" : {"$avg" : "$m"}, "min" : {"$min" : "$n"}, "max" : {"$max" : "$n"}, "first" : {"$first" : "$s"}, "last" : {"$last" : "$s"}, "push" : {"$push" : "$m"}, "set" : {"$addToSet" : "$m"}, "firstN" : {"$firstN" : {"input" : "$m", "n" : 1}}, "counted" : {"$count" : {}}}}]);

SET dialect='clickhouse';
DROP TABLE one;
