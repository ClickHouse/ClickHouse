DROP TABLE IF EXISTS tt_error_1373;

CREATE TABLE tt_error_1373
( a   Int64, d   Int64, val Int64 ) 
ENGINE = SummingMergeTree((a, val)) PARTITION BY (a) ORDER BY (d); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tt_error_1373
( a   Int64, d   Int64, val Int64 )
ENGINE = SummingMergeTree((a, val)) PARTITION BY (a % 5) ORDER BY (d); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tt_error_1373
( a   Int64, d   Int64, val Int64 )
    ENGINE = SummingMergeTree((d, val)) PARTITION BY (a) ORDER BY (d); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tt_error_1373
( a   Int64, d   Int64, val Int64 )
    ENGINE = SummingMergeTree((d, val)) PARTITION BY (a) ORDER BY (d % 5); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS tt_error_1373;