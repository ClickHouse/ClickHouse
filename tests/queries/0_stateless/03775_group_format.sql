set output_format_write_statistics=0;
set max_threads=1;

select groupFormat('JSONEachRow')(number, toString(number)) from numbers(3);

set format_csv_delimiter=';';
select groupFormat('CSVWithNamesAndTypes')(number, toString(number)) from numbers(2);

select groupFormat('JSONCompactColumns')(number, toString(number)) from numbers(2);

set output_format_json_quote_64bit_integers=1;
select groupFormat('JSONEachRow')(number) from numbers(2);
set output_format_json_quote_64bit_integers=0;

select groupFormat('JSONEachRow')(if(number = 0, NULL, number)) from numbers(2);

select groupFormat('JSONEachRow')(number) from (select number from numbers(3) order by number desc);

select key, groupFormat('JSONEachRow')(number)
from (select number, number % 2 as key from numbers(4) order by number)
group by key
order by key;

select groupFormat('JSONEachRow')(number) from numbers(0);

select groupFormatIf('JSONEachRow')(
    if(number = 0, NULL, number),
    if(number = 1, CAST(NULL, 'Nullable(UInt8)'), toUInt8(number != 2)))
from numbers(4);

-- Verify that nullable payload is preserved when the optimizer could try to rewrite f(if(..., NULL, x)) to fIf(x, cond).
set optimize_rewrite_aggregate_function_with_if = 1;
select groupFormat('JSONEachRow')(if(number = 0, NULL, number)) from numbers(2);
set optimize_rewrite_aggregate_function_with_if = 0;

-- Multi-arg nullable: both nullable and non-nullable columns mixed.
select groupFormat('JSONEachRow')(if(number = 0, NULL, number), toString(number)) from numbers(3);

-- `OrNull` / `OrDefault` wrappers must also preserve nullable payload (e.g. `{"c1":null}`),
-- not strip it via the `Null` combinator fallback.
select groupFormatOrNull('JSONEachRow')(if(number = 0, NULL, number)) from numbers(2);
select groupFormatOrDefault('JSONEachRow')(if(number = 0, NULL, number)) from numbers(2);
-- `OrNull` returns NULL and `OrDefault` returns the default value for an empty group.
select groupFormatOrNull('JSONEachRow')(number) from numbers(0);
select groupFormatOrDefault('JSONEachRow')(number) from numbers(0);

-- State round-trip: serialize then deserialize via finalizeAggregation.
select finalizeAggregation(groupFormatState('JSONEachRow')(number, toString(number))) from numbers(3);

-- State merge: two partial states merged via groupFormatMerge (order-independent check).
select
    position(result, '{"c1":0}') > 0 and
    position(result, '{"c1":1}') > 0 and
    position(result, '{"c1":2}') > 0 and
    position(result, '{"c1":3}') > 0
from
(
    select groupFormatMerge('JSONEachRow')(state) as result from
    (
        select groupFormatState('JSONEachRow')(number) as state from numbers(2)
        union all
        select groupFormatState('JSONEachRow')(number + 2) as state from numbers(2)
    )
);

-- Nullable state round-trip.
select finalizeAggregation(groupFormatState('JSONEachRow')(if(number = 0, NULL, number))) from numbers(3);

-- Equivalence: direct aggregation vs state round-trip must produce the same result.
select
    groupFormat('JSONEachRow')(number) as direct,
    finalizeAggregation(groupFormatState('JSONEachRow')(number)) as via_state,
    direct = via_state as equal
from numbers(3);

-- NULL handling contract: a typed `Nullable(T)` payload is preserved as `null`
-- in the formatted output, while a literal untyped `NULL` (`Nullable(Nothing)`)
-- is replaced by the `Null` combinator with a `NULL`-returning placeholder
-- before `groupFormat` can rebuild itself with the nullable argument types,
-- so the whole aggregate returns `NULL`. See `docs/.../groupFormat.md`.
select groupFormat('JSONEachRow')(CAST(NULL, 'Nullable(UInt8)')) from numbers(1);
select groupFormat('JSONEachRow')(if(number = 1, CAST(NULL, 'Nullable(UInt8)'), toNullable(toUInt8(number)))) from numbers(3);
select groupFormat('JSONEachRow')(NULL) from numbers(1);
select groupFormat('JSONEachRow')(NULL) from numbers(3);

select groupFormat(123)(number) from numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select groupFormat() from numbers(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
