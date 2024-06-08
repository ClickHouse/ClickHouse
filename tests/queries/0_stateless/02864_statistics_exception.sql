DROP TABLE IF EXISTS t1;

CREATE TABLE t1 
(
    a Float64 STATISTICS(tdigest),
    b Int64 STATISTICS(tdigest),
    pk String,
) Engine = MergeTree() ORDER BY pk; -- { serverError INCORRECT_QUERY }

SET allow_experimental_statistics = 1;

CREATE TABLE t1 
(
    a Float64 STATISTICS(tdigest),
    b Int64,
    pk String STATISTICS(tdigest),
) Engine = MergeTree() ORDER BY pk; -- { serverError ILLEGAL_STATISTICS }

CREATE TABLE t1 
(
    a Float64 STATISTICS(tdigest, tdigest(10)),
    b Int64,
) Engine = MergeTree() ORDER BY pk; -- { serverError INCORRECT_QUERY }

CREATE TABLE t1 
(
    a Float64 STATISTICS(xyz),
    b Int64,
) Engine = MergeTree() ORDER BY pk; -- { serverError INCORRECT_QUERY }

CREATE TABLE t1 
(
    a Float64,
    b Int64,
    pk String,
) Engine = MergeTree() ORDER BY pk; 

ALTER TABLE t1 ADD STATISTICS a TYPE xyz; -- { serverError INCORRECT_QUERY }
ALTER TABLE t1 ADD STATISTICS a TYPE tdigest;
ALTER TABLE t1 ADD STATISTICS IF NOT EXISTS a TYPE tdigest;
ALTER TABLE t1 ADD STATISTICS a TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
-- Statistics can be created only on integer columns
ALTER TABLE t1 MODIFY STATISTICS a TYPE tdigest;
ALTER TABLE t1 ADD STATISTICS pk TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE t1 DROP STATISTICS b; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE t1 DROP STATISTICS a;
ALTER TABLE t1 DROP STATISTICS IF EXISTS a;
ALTER TABLE t1 CLEAR STATISTICS a; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE t1 CLEAR STATISTICS IF EXISTS a;
ALTER TABLE t1 MATERIALIZE STATISTICS b; -- { serverError ILLEGAL_STATISTICS }

ALTER TABLE t1 ADD STATISTICS a TYPE tdigest;
ALTER TABLE t1 ADD STATISTICS b TYPE tdigest;
ALTER TABLE t1 MODIFY COLUMN a Float64 TTL toDateTime(b) + INTERVAL 1 MONTH;
ALTER TABLE t1 MODIFY COLUMN a Int64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t1;
