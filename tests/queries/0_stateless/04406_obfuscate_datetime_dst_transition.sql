-- On a DST transition day the local day is not 24 hours long, so re-attaching an obfuscated time of
-- day to the source date as a raw "seconds since midnight" offset added to the source midnight could
-- spill onto a neighbouring displayed date. `DateTimeModel` instead rebuilds the result from the
-- source date plus the obfuscated local wall-clock time in the column's timezone (via
-- `DateLUTImpl::makeDateTime`), so the displayed date is always preserved.
--
-- 'Europe/London' springs forward on 2024-03-31 (the clock jumps 01:00 -> 02:00), making that local
-- day only 23 hours long. All inputs below are on the London date 2024-03-31 and straddle the missing
-- hour; every obfuscated value must keep the date 2024-03-31 as displayed in 'Europe/London'.
--
-- `obfuscate(...)` is an effectively infinite, repeating source, so the inner subquery needs an
-- explicit `LIMIT`; one full pass over the input rows is enough to observe every preserved date.

SELECT DISTINCT toDate(x, 'Europe/London')
FROM (
    SELECT * FROM obfuscate(
        SELECT toDateTime('2024-03-31 00:00:00', 'Europe/London') + toIntervalSecond(number * 37) AS x
        FROM numbers(1000)) LIMIT 1000
    SETTINGS obfuscate_seed = 'obfuscate_dst_transition');
