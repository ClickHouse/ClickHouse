DROP TABLE IF EXISTS tt_error_1373;

CREATE TABLE tt_error_1373
( a   Int64, d   Int64, val Int64 ) 
ENGINE = SummingMergeTree((a, val)) PARTITION BY (a) ORDER BY (d); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS tt_error_1373;