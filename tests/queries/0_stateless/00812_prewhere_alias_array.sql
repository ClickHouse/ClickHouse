DROP TABLE IF EXISTS prewhere;
CREATE TABLE prewhere (x Array(UInt64), y ALIAS x, s String) ENGINE = MergeTree ORDER BY tuple();
SELECT count() FROM prewhere PREWHERE (length(s) >= 1) = 0 WHERE NOT ignore(y);
DROP TABLE prewhere;
