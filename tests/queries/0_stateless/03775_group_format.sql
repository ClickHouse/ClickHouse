set output_format_write_statistics=0;
set max_threads=1;

select groupFormat('JSONEachRow')(number, toString(number)) from numbers(3);

set format_csv_delimiter=';';
select groupFormat('CSVWithNamesAndTypes')(number, toString(number)) from numbers(2);

select groupFormat('JSONCompactColumns')(number, toString(number)) from numbers(2);

set output_format_json_quote_64bit_integers=1;
select groupFormat('JSONEachRow')(number) from numbers(2);
set output_format_json_quote_64bit_integers=0;

-- Input order is preserved.
select groupFormat('JSONEachRow')(number) from (select number from numbers(3) order by number desc);

select key, groupFormat('JSONEachRow')(number)
from (select number, number % 2 as key from numbers(4) order by number)
group by key
order by key;

-- An empty group is formatted as a zero-row block; the exact output is format-specific
-- and left unspecified - `JSONEachRow` happens to produce an empty string.
select groupFormat('JSONEachRow')(number) from numbers(0);

-- NULL handling: like `groupArray` and `groupConcat`, a row is skipped when any argument is NULL.
select groupFormat('JSONEachRow')(if(number = 0, NULL, number)) from numbers(3);
-- Multi-argument: the whole row is skipped if any of its columns is NULL.
select groupFormat('JSONEachRow')(if(number = 0, NULL, number), toString(number)) from numbers(3);
-- When an argument is nullable, the result type is `Nullable(String)`; a group whose
-- every row is skipped returns NULL (the generic `Null` combinator returns NULL when
-- nothing was aggregated), not an empty string.
select groupFormat('JSONEachRow')(CAST(NULL, 'Nullable(UInt8)')) from numbers(2);
-- A literal untyped NULL argument (`Nullable(Nothing)`) makes the whole aggregate return NULL
-- via the generic `Null` combinator, like other aggregate functions.
select groupFormat('JSONEachRow')(NULL) from numbers(3);

-- The rewrite f(if(cond, NULL, x)) -> fIf(x, !cond) does not change the result here,
-- because rows with a NULL argument are skipped either way.
select groupFormat('JSONEachRow')(if(number = 0, NULL, number)) from numbers(3) settings optimize_rewrite_aggregate_function_with_if = 1;
select groupFormat('JSONEachRow')(if(number = 0, NULL, number)) from numbers(3) settings optimize_rewrite_aggregate_function_with_if = 0;

-- Finalize an in-memory state. This does not exercise the binary `serialize` / `deserialize`;
-- see `04342_group_format_serialization` for the on-disk aggregate-state round-trip.
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

-- Equivalence: direct aggregation vs in-memory state finalization must produce the same result.
select
    groupFormat('JSONEachRow')(number) as direct,
    finalizeAggregation(groupFormatState('JSONEachRow')(number)) as via_state,
    direct = via_state as equal
from numbers(3);

-- The nested formatter must not leak the enclosing query's `statistics` section into the
-- formatted output, even when `output_format_write_statistics` is enabled for the query.
select position(groupFormat('JSON')(number), 'statistics') = 0 from numbers(2) settings output_format_write_statistics = 1;

select groupFormat(123)(number) from numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select groupFormat() from numbers(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
