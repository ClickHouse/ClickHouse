-- Tags: long

-- { echo }

-- just something basic
select number, count() over (partition by intDiv(number, 3) order by number rows unbounded preceding) from numbers(10);

-- proper calculation across blocks
select number, max(number) over (partition by intDiv(number, 3) order by number desc rows unbounded preceding) from numbers(10) settings max_block_size = 2;

-- not a window function
select number, abs(number) over (partition by toString(intDiv(number, 3)) rows unbounded preceding) from numbers(10); -- { serverError 63 }

-- no partition by
select number, avg(number) over (order by number rows unbounded preceding) from numbers(10);

-- no order by
select number, quantileExact(number) over (partition by intDiv(number, 3) AS value order by number rows unbounded preceding) from numbers(10);

-- can add an alias after window spec
select number, quantileExact(number) over (partition by intDiv(number, 3) AS value order by number rows unbounded preceding) q from numbers(10);

-- can't reference it yet -- the window functions are calculated at the
-- last stage of select, after all other functions.
select q * 10, quantileExact(number) over (partition by intDiv(number, 3) rows unbounded preceding) q from numbers(10); -- { serverError 47 }

-- must work in WHERE if you wrap it in a subquery
select * from (select count(*) over (rows unbounded preceding) c from numbers(3)) where c > 0;

-- should work in ORDER BY
select number, max(number) over (partition by intDiv(number, 3) order by number desc rows unbounded preceding) m from numbers(10) order by m desc, number;

-- also works in ORDER BY if you wrap it in a subquery
select * from (select count(*) over (rows unbounded preceding) c from numbers(3)) order by c;

-- Example with window function only in ORDER BY. Here we make a rank of all
-- numbers sorted descending, and then sort by this rank descending, and must get
-- the ascending order.
select * from (select * from numbers(5) order by rand()) order by count() over (order by number desc rows unbounded preceding) desc;

-- Aggregate functions as window function arguments. This query is semantically
-- the same as the above one, only we replace `number` with
-- `any(number) group by number` and so on.
select * from (select * from numbers(5) order by rand()) group by number order by sum(any(number + 1)) over (order by min(number) desc rows unbounded preceding) desc;
-- some more simple cases w/aggregate functions
select sum(any(number)) over (rows unbounded preceding) from numbers(1);
select sum(any(number) + 1) over (rows unbounded preceding) from numbers(1);
select sum(any(number + 1)) over (rows unbounded preceding) from numbers(1);

-- different windows
-- an explain test would also be helpful, but it's too immature now and I don't
-- want to change reference all the time
select number, max(number) over (partition by intDiv(number, 3) order by number desc rows unbounded preceding), count(number) over (partition by intDiv(number, 5) order by number rows unbounded preceding) as m from numbers(31) order by number settings max_block_size = 2;

-- two functions over the same window
-- an explain test would also be helpful, but it's too immature now and I don't
-- want to change reference all the time
select number, max(number) over (partition by intDiv(number, 3) order by number desc rows unbounded preceding), count(number) over (partition by intDiv(number, 3) order by number desc rows unbounded preceding) as m from numbers(7) order by number settings max_block_size = 2;

-- check that we can work with constant columns
select median(x) over (partition by x) from (select 1 x);

-- an empty window definition is valid as well
select groupArray(number) over (rows unbounded preceding) from numbers(3);
select groupArray(number) over () from numbers(3);

-- This one tests we properly process the window  function arguments.
-- Seen errors like 'column `1` not found' from count(1).
select count(1) over (rows unbounded preceding), max(number + 1) over () from numbers(3);

-- Should work in DISTINCT
select distinct sum(0) over (rows unbounded preceding) from numbers(2);
select distinct any(number) over (rows unbounded preceding) from numbers(2);

-- Various kinds of aliases are properly substituted into various parts of window
-- function definition.
with number + 1 as x select intDiv(number, 3) as y, sum(x + y) over (partition by y order by x rows unbounded preceding) from numbers(7);

-- WINDOW clause
select 1 window w1 as ();

select sum(number) over w1, sum(number) over w2
from numbers(10)
window
    w1 as (rows unbounded preceding),
    w2 as (partition by intDiv(number, 3) as value order by number rows unbounded preceding)
;

-- FIXME both functions should use the same window, but they don't. Add an
-- EXPLAIN test for this.
select
    sum(number) over w1,
    sum(number) over (partition by intDiv(number, 3) as value order by number rows unbounded preceding)
from numbers(10)
window
    w1 as (partition by intDiv(number, 3) rows unbounded preceding)
;

-- RANGE frame
-- It's the default
select sum(number) over () from numbers(3);

-- Try some mutually prime sizes of partition, group and block, for the number
-- of rows that is their least common multiple + 1, so that we see all the
-- interesting corner cases.
select number, intDiv(number, 3) p, mod(number, 2) o, count(number) over w as c
from numbers(31)
window w as (partition by p order by o, number range unbounded preceding)
order by number
settings max_block_size = 5
;

select number, intDiv(number, 5) p, mod(number, 3) o, count(number) over w as c
from numbers(31)
window w as (partition by p order by o, number range unbounded preceding)
order by number
settings max_block_size = 2
;

select number, intDiv(number, 5) p, mod(number, 2) o, count(number) over w as c
from numbers(31)
window w as (partition by p order by o, number range unbounded preceding)
order by number
settings max_block_size = 3
;

select number, intDiv(number, 3) p, mod(number, 5) o, count(number) over w as c
from numbers(31)
window w as (partition by p order by o, number range unbounded preceding)
order by number
settings max_block_size = 2
;

select number, intDiv(number, 2) p, mod(number, 5) o, count(number) over w as c
from numbers(31)
window w as (partition by p order by o, number range unbounded preceding)
order by number
settings max_block_size = 3
;

select number, intDiv(number, 2) p, mod(number, 3) o, count(number) over w as c
from numbers(31)
window w as (partition by p order by o range unbounded preceding)
order by number
settings max_block_size = 5
;

-- A case where the partition end is in the current block, and the frame end
-- is triggered by the partition end.
select min(number) over (partition by p)  from (select number, intDiv(number, 3) p from numbers(10));

-- UNBOUNDED FOLLOWING frame end
select
    min(number) over wa, min(number) over wo,
    max(number) over wa, max(number) over wo
from
    (select number, intDiv(number, 3) p, mod(number, 5) o
        from numbers(31))
window
    wa as (partition by p order by o
        range between unbounded preceding and unbounded following),
    wo as (partition by p order by o
        rows between unbounded preceding and unbounded following)
settings max_block_size = 2;

-- ROWS offset frame start
select number, p,
    count(*) over (partition by p order by number
        rows between 1 preceding and unbounded following),
    count(*) over (partition by p order by number
        rows between current row and unbounded following),
    count(*) over (partition by p order by number
        rows between 1 following and unbounded following)
from (select number, intDiv(number, 5) p from numbers(31))
order by p, number
settings max_block_size = 2;

-- ROWS offset frame start and end
select number, p,
    count(*) over (partition by p order by number
        rows between 2 preceding and 2 following)
from (select number, intDiv(number, 7) p from numbers(71))
order by p, number
settings max_block_size = 2;

SELECT count(*) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM numbers(4);

-- frame boundaries that runs into the partition end
select
    count() over (partition by intDiv(number, 3)
        rows between 100 following and unbounded following),
    count() over (partition by intDiv(number, 3)
        rows between current row and 100 following)
from numbers(10);

-- seen a use-after-free under MSan in this query once
SELECT number, max(number) OVER (PARTITION BY intDiv(number, 7) ORDER BY number ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM numbers(1024) SETTINGS max_block_size = 2 FORMAT Null;

-- a corner case
select count() over ();

-- RANGE CURRENT ROW frame start
select number, p, o,
    count(*) over (partition by p order by o
        range between current row and unbounded following)
from (select number, intDiv(number, 5) p, mod(number, 3) o
    from numbers(31))
order by p, o, number
settings max_block_size = 2;

select
    count(*) over (rows between  current row and current row),
    count(*) over (range between  current row and current row)
from numbers(3);

-- RANGE OFFSET
-- a basic RANGE OFFSET frame
select x, min(x) over w, max(x) over w, count(x) over w from (
    select toUInt8(number) x from numbers(11))
window w as (order by x asc range between 1 preceding and 2 following)
order by x;

-- overflow conditions
select x, min(x) over w, max(x) over w, count(x) over w
from (
    select toUInt8(if(mod(number, 2),
        toInt64(255 - intDiv(number, 2)),
        toInt64(intDiv(number, 2)))) x
    from numbers(10)
)
window w as (order by x range between 1 preceding and 2 following)
order by x;

select x, min(x) over w, max(x) over w, count(x) over w
from (
    select toInt8(multiIf(
        mod(number, 3) == 0, toInt64(intDiv(number, 3)),
        mod(number, 3) == 1, toInt64(127 - intDiv(number, 3)),
        toInt64(-128 + intDiv(number, 3)))) x
    from numbers(15)
)
window w as (order by x range between 1 preceding and 2 following)
order by x;

-- We need large offsets to trigger overflow to positive direction, or
-- else the frame end runs into partition end w/o overflow and doesn't move
-- after that. The frame from this query is equivalent to the entire partition.
select x, min(x) over w, max(x) over w, count(x) over w
from (
    select toUInt8(if(mod(number, 2),
        toInt64(255 - intDiv(number, 2)),
        toInt64(intDiv(number, 2)))) x
    from numbers(10)
)
window w as (order by x range between 255 preceding and 255 following)
order by x;

-- RANGE OFFSET ORDER BY DESC
select x, min(x) over w, max(x) over w, count(x) over w from (
    select toUInt8(number) x from numbers(11)) t
window w as (order by x desc range between 1 preceding and 2 following)
order by x
settings max_block_size = 1;

select x, min(x) over w, max(x) over w, count(x) over w from (
    select toUInt8(number) x from numbers(11)) t
window w as (order by x desc range between 1 preceding and unbounded following)
order by x
settings max_block_size = 2;

select x, min(x) over w, max(x) over w, count(x) over w from (
    select toUInt8(number) x from numbers(11)) t
window w as (order by x desc range between unbounded preceding and 2 following)
order by x
settings max_block_size = 3;

select x, min(x) over w, max(x) over w, count(x) over w from (
    select toUInt8(number) x from numbers(11)) t
window w as (order by x desc range between unbounded preceding and 2 preceding)
order by x
settings max_block_size = 4;


-- Check that we put windows in such an order that we can reuse the sort.
-- First, check that at least the result is correct when we have many windows
-- with different sort order.
select
    number,
    count(*) over (partition by p order by number),
    count(*) over (partition by p order by number, o),
    count(*) over (),
    count(*) over (order by number),
    count(*) over (order by o),
    count(*) over (order by o, number),
    count(*) over (order by number, o),
    count(*) over (partition by p order by o, number),
    count(*) over (partition by p),
    count(*) over (partition by p order by o),
    count(*) over (partition by p, o order by number)
from
    (select number, intDiv(number, 3) p, mod(number, 5) o
        from numbers(16)) t
order by number
;

-- The EXPLAIN for the above query would be difficult to understand, so check some
-- simple cases instead.
explain select
    count(*) over (partition by p),
    count(*) over (),
    count(*) over (partition by p order by o)
from
    (select number, intDiv(number, 3) p, mod(number, 5) o
        from numbers(16)) t
;

explain select
    count(*) over (order by o, number),
    count(*) over (order by number)
from
    (select number, intDiv(number, 3) p, mod(number, 5) o
        from numbers(16)) t
;

-- A test case for the sort comparator found by fuzzer.
SELECT
    max(number) OVER (ORDER BY number DESC NULLS FIRST),
    max(number) OVER (ORDER BY number ASC NULLS FIRST)
FROM numbers(2)
;

-- optimize_read_in_order conflicts with sorting for window functions, check that
-- it is disabled.
drop table if exists window_mt;
create table window_mt engine MergeTree order by number
    as select number, mod(number, 3) p from numbers(100);

select number, count(*) over (partition by p)
    from window_mt order by number limit 10 settings optimize_read_in_order = 0;

select number, count(*) over (partition by p)
    from window_mt order by number limit 10 settings optimize_read_in_order = 1;

drop table window_mt;

-- some true window functions -- rank and friends
select number, p, o,
    count(*) over w,
    rank() over w,
    dense_rank() over w,
    row_number() over w
from (select number, intDiv(number, 5) p, mod(number, 3) o
    from numbers(31) order by o, number) t
window w as (partition by p order by o, number)
order by p, o, number
settings max_block_size = 2;

-- our replacement for lag/lead
select
    anyOrNull(number)
        over (order by number rows between 1 preceding and 1 preceding),
    anyOrNull(number)
        over (order by number rows between 1 following and 1 following)
from numbers(5);

-- variants of lag/lead that respect the frame
select number, p, pp,
    lagInFrame(number) over w as lag1,
    lagInFrame(number, number - pp) over w as lag2,
    lagInFrame(number, number - pp, number * 11) over w as lag,
    leadInFrame(number, number - pp, number * 11) over w as lead
from (select number, intDiv(number, 5) p, p * 5 pp from numbers(16))
window w as (partition by p order by number
    rows between unbounded preceding and unbounded following)
order by number
settings max_block_size = 3;
;

-- careful with auto-application of Null combinator
select lagInFrame(toNullable(1)) over ();
select lagInFrameOrNull(1) over (); -- { serverError 36 }
-- this is the same as `select max(Null::Nullable(Nothing))`
select intDiv(1, NULL) x, toTypeName(x), max(x) over ();
-- to make lagInFrame return null for out-of-frame rows, cast the argument to
-- Nullable; otherwise, it returns default values.
SELECT
    number,
    lagInFrame(toNullable(number), 1) OVER w,
    lagInFrame(toNullable(number), 2) OVER w,
    lagInFrame(number, 1) OVER w,
    lagInFrame(number, 2) OVER w
FROM numbers(4)
WINDOW w AS (ORDER BY number ASC)
;

-- case-insensitive SQL-standard synonyms for any and anyLast
select
    number,
    fIrSt_VaLue(number) over w,
    lAsT_vAlUe(number) over w
from numbers(10)
window w as (order by number range between 1 preceding and 1 following)
order by number
;

-- lagInFrame UBsan
SELECT lagInFrame(1, -1) OVER (); -- { serverError BAD_ARGUMENTS }
SELECT lagInFrame(1, 0) OVER ();
SELECT lagInFrame(1, /* INT64_MAX+1 */ 0x7fffffffffffffff+1) OVER (); -- { serverError BAD_ARGUMENTS }
SELECT lagInFrame(1, /* INT64_MAX */ 0x7fffffffffffffff) OVER ();
SELECT lagInFrame(1, 1) OVER ();

-- leadInFrame UBsan
SELECT leadInFrame(1, -1) OVER (); -- { serverError BAD_ARGUMENTS }
SELECT leadInFrame(1, 0) OVER ();
SELECT leadInFrame(1, /* INT64_MAX+1 */ 0x7fffffffffffffff+1) OVER (); -- { serverError BAD_ARGUMENTS }
SELECT leadInFrame(1, /* INT64_MAX */ 0x7fffffffffffffff) OVER ();
SELECT leadInFrame(1, 1) OVER ();

-- In this case, we had a problem with PartialSortingTransform returning zero-row
-- chunks for input chunks w/o columns.
select count() over () from numbers(4) where number < 2;

-- floating point RANGE frame
select
    count(*) over (order by toFloat32(number) range 5. preceding),
    count(*) over (order by toFloat64(number) range 5. preceding),
    count(*) over (order by toFloat32(number) range between current row and 5. following),
    count(*) over (order by toFloat64(number) range between current row and 5. following)
from numbers(7)
;

-- negative offsets should not be allowed
select count() over (order by toInt64(number) range between -1 preceding and unbounded following) from numbers(1); -- { serverError 36 }
select count() over (order by toInt64(number) range between -1 following and unbounded following) from numbers(1); -- { serverError 36 }
select count() over (order by toInt64(number) range between unbounded preceding and -1 preceding) from numbers(1); -- { serverError 36 }
select count() over (order by toInt64(number) range between unbounded preceding and -1 following) from numbers(1); -- { serverError 36 }

-- a test with aggregate function that allocates memory in arena
select sum(a[length(a)])
from (
    select groupArray(number) over (partition by modulo(number, 11)
            order by modulo(number, 1111), number) a
    from numbers_mt(10000)
) settings max_block_size = 7;

-- a test with aggregate function which is -state type
select bitmapCardinality(bs)
from
    (
        select groupBitmapMergeState(bm) over (order by k asc rows between unbounded preceding and current row) as bs
        from
            (
                select
                    groupBitmapState(number) as bm, k
                from
                    (
                        select
                            number,
                            number % 3 as k
                        from numbers(3)
                    )
                group by k
            )
    );

-- -INT_MIN row offset that can lead to problems with negation, found when fuzzing
-- under UBSan. Should be limited to at most INT_MAX.
select count() over (rows between 2147483648 preceding and 2147493648 following) from numbers(2); -- { serverError 36 }

-- Somehow in this case WindowTransform gets empty input chunks not marked as
-- input end, and then two (!) empty input chunks marked as input end. Whatever.
select count() over () from (select 1 a) l inner join (select 2 a) r using a;
-- This case works as expected, one empty input chunk marked as input end.
select count() over () where null;

-- Inheriting another window.
select number, count() over (w1 rows unbounded preceding) from numbers(10)
window
    w0 as (partition by intDiv(number, 5) as p),
    w1 as (w0 order by mod(number, 3) as o, number)
order by p, o, number
;

-- can't redefine PARTITION BY
select count() over (w partition by number) from numbers(1) window w as (partition by intDiv(number, 5)); -- { serverError 36 }

-- can't redefine existing ORDER BY
select count() over (w order by number) from numbers(1) window w as (partition by intDiv(number, 5) order by mod(number, 3)); -- { serverError 36 }

-- parent window can't have frame
select count() over (w range unbounded preceding) from numbers(1) window w as (partition by intDiv(number, 5) order by mod(number, 3) rows unbounded preceding); -- { serverError 36 }

-- looks weird but probably should work -- this is a window that inherits and changes nothing
select count() over (w) from numbers(1) window w as ();

-- nonexistent parent window
select count() over (w2 rows unbounded preceding); -- { serverError 36 }
