-- Tests creating/dropping/materializing statistics produces the right exceptions.

DROP TABLE IF EXISTS tab;

-- Can't create statistics when allow_experimental_statistics = 0
CREATE TABLE tab
(
    a Float64 STATISTICS(tdigest)
) Engine = MergeTree() ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

SET allow_experimental_statistics = 1;

-- The same type of statistics can't exist more than once on a column
CREATE TABLE tab
(
    a Float64 STATISTICS(tdigest, tdigest)
) Engine = MergeTree() ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

-- Unknown statistics types are rejected
CREATE TABLE tab
(
    a Float64 STATISTICS(no_statistics_type)
) Engine = MergeTree() ORDER BY tuple(); -- { serverError INCORRECT_QUERY }

-- tDigest statistics can only be created on numeric columns
CREATE TABLE tab
(
    a String STATISTICS(tdigest),
) Engine = MergeTree() ORDER BY tuple(); -- { serverError ILLEGAL_STATISTICS }

CREATE TABLE tab
(
    a Float64,
    b String
) Engine = MergeTree() ORDER BY tuple();

ALTER TABLE tab ADD STATISTICS a TYPE no_statistics_type; -- { serverError INCORRECT_QUERY }
ALTER TABLE tab ADD STATISTICS a TYPE tdigest;
ALTER TABLE tab ADD STATISTICS IF NOT EXISTS a TYPE tdigest;
ALTER TABLE tab ADD STATISTICS a TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab MODIFY STATISTICS a TYPE tdigest;
-- Statistics can be created only on integer columns
ALTER TABLE tab ADD STATISTICS b TYPE tdigest; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab DROP STATISTICS b; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab DROP STATISTICS a;
ALTER TABLE tab DROP STATISTICS IF EXISTS a;
ALTER TABLE tab CLEAR STATISTICS a; -- { serverError ILLEGAL_STATISTICS }
ALTER TABLE tab CLEAR STATISTICS IF EXISTS a;
ALTER TABLE tab MATERIALIZE STATISTICS b; -- { serverError ILLEGAL_STATISTICS }

ALTER TABLE tab ADD STATISTICS a TYPE tdigest;
ALTER TABLE tab MODIFY COLUMN a Float64 TTL toDateTime(b) + INTERVAL 1 MONTH;
ALTER TABLE tab MODIFY COLUMN a Int64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE tab;
