DROP TABLE IF EXISTS testNullableStates;
DROP TABLE IF EXISTS testNullableStatesAgg;

CREATE TABLE testNullableStates (
   ts DateTime,
   id String,
   string Nullable(String),
   float64 Nullable(Float64),
   float32 Nullable(Float32),
   decimal325 Nullable(Decimal32(5)),
   date Nullable(Date),
   datetime Nullable(DateTime),
   datetime64 Nullable(DateTime64),
   int64 Nullable(Int64),
   int32 Nullable(Int32),
   int16 Nullable(Int16),
   int8 Nullable(Int8))
ENGINE=MergeTree PARTITION BY toStartOfDay(ts) ORDER BY id;

INSERT INTO testNullableStates SELECT
    toDateTime('2020-01-01 00:00:00') + number AS ts,
    toString(number % 999) AS id,
    toString(number) AS string,
    number / 333 AS float64,
    number / 333 AS float32,
    number / 333 AS decimal325,
    toDate(ts),
    ts,
    ts,
    number,
    toInt32(number),
    toInt16(number),
    toInt8(number)
FROM numbers(100000);

INSERT INTO testNullableStates SELECT
    toDateTime('2020-01-01 00:00:00') + number AS ts,
    toString(number % 999 - 5) AS id,
    NULL AS string,
    NULL AS float64,
    NULL AS float32,
    NULL AS decimal325,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
FROM numbers(500);


CREATE TABLE testNullableStatesAgg
(
    `ts` DateTime,
    `id` String,
    `stringMin` AggregateFunction(min, Nullable(String)),
    `stringMax` AggregateFunction(max, Nullable(String)),
    `float64Min` AggregateFunction(min, Nullable(Float64)),
    `float64Max` AggregateFunction(max, Nullable(Float64)),
    `float64Avg` AggregateFunction(avg, Nullable(Float64)),
    `float64Sum` AggregateFunction(sum, Nullable(Float64)),
    `float32Min` AggregateFunction(min, Nullable(Float32)),
    `float32Max` AggregateFunction(max, Nullable(Float32)),
    `float32Avg` AggregateFunction(avg, Nullable(Float32)),
    `float32Sum` AggregateFunction(sum, Nullable(Float32)),
    `decimal325Min` AggregateFunction(min, Nullable(Decimal32(5))),
    `decimal325Max` AggregateFunction(max, Nullable(Decimal32(5))),
    `decimal325Avg` AggregateFunction(avg, Nullable(Decimal32(5))),
    `decimal325Sum` AggregateFunction(sum, Nullable(Decimal32(5))),
    `dateMin` AggregateFunction(min, Nullable(Date)),
    `dateMax` AggregateFunction(max, Nullable(Date)),
    `datetimeMin` AggregateFunction(min, Nullable(DateTime)),
    `datetimeMax` AggregateFunction(max, Nullable(DateTime)),
    `datetime64Min` AggregateFunction(min, Nullable(datetime64)),
    `datetime64Max` AggregateFunction(max, Nullable(datetime64)),
    `int64Min` AggregateFunction(min, Nullable(Int64)),
    `int64Max` AggregateFunction(max, Nullable(Int64)),
    `int64Avg` AggregateFunction(avg, Nullable(Int64)),
    `int64Sum` AggregateFunction(sum, Nullable(Int64)),
    `int32Min` AggregateFunction(min, Nullable(Int32)),
    `int32Max` AggregateFunction(max, Nullable(Int32)),
    `int32Avg` AggregateFunction(avg, Nullable(Int32)),
    `int32Sum` AggregateFunction(sum, Nullable(Int32)),
    `int16Min` AggregateFunction(min, Nullable(Int16)),
    `int16Max` AggregateFunction(max, Nullable(Int16)),
    `int16Avg` AggregateFunction(avg, Nullable(Int16)),
    `int16Sum` AggregateFunction(sum, Nullable(Int16)),
    `int8Min` AggregateFunction(min, Nullable(Int8)),
    `int8Max` AggregateFunction(max, Nullable(Int8)),
    `int8Avg` AggregateFunction(avg, Nullable(Int8)),
    `int8Sum` AggregateFunction(sum, Nullable(Int8))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toStartOfDay(ts)
ORDER BY id;




insert into testNullableStatesAgg
select
   ts DateTime,
   id String,
   minState(string) stringMin,
   maxState(string) stringMax,
   minState(float64) float64Min,
   maxState(float64) float64Max,
   avgState(float64) float64Avg,
   sumState(float64) float64Sum,
   minState(float32) float32Min,
   maxState(float32) float32Max,
   avgState(float32) float32Avg,
   sumState(float32) float32Sum,
   minState(decimal325) decimal325Min,
   maxState(decimal325) decimal325Max,
   avgState(decimal325) decimal325Avg,
   sumState(decimal325) decimal325Sum,
   minState(date) dateMin,
   maxState(date) dateMax,
   minState(datetime) datetimeMin,
   maxState(datetime) datetimeMax,
   minState(datetime64) datetime64Min,
   maxState(datetime64) datetime64Max,
   minState(int64) int64Min,
   maxState(int64) int64Max,
   avgState(int64) int64Avg,
   sumState(int64) int64Sum,
   minState(int32) int32Min,
   maxState(int32) int32Max,
   avgState(int32) int32Avg,
   sumState(int32) int32Sum,
   minState(int16) int16Min,
   maxState(int16) int16Max,
   avgState(int16) int16Avg,
   sumState(int16) int16Sum,
   minState(int8) int8Min,
   maxState(int8) int8Max,
   avgState(int8) int8Avg,
   sumState(int8) int8Sum
from testNullableStates
group by ts, id;

OPTIMIZE TABLE testNullableStatesAgg FINAL;

select count() from testNullableStates;

select count() from testNullableStatesAgg;

select ' ---- select without states ---- ';

SELECT id, count(),
    min(string),
    max(string),
    round(min(float64),5),
    round(max(float64),5),
    round(avg(float64),5),
    round(sum(float64),5),
    round(min(float32),5),
    round(max(float32),5),
    round(avg(float32),5),
    round(sum(float32),5),
    min(decimal325),
    max(decimal325),
    round(avg(decimal325), 5),
    sum(decimal325),
    min(date),
    max(date),
    min(datetime),
    max(datetime),
    min(datetime64),
    max(datetime64),
    min(int64),
    max(int64),
    round(avg(int64), 5),
    sum(int64),
    min(int32),
    max(int32),
    round(avg(int32), 5),
    sum(int32),
    min(int16),
    max(int16),
    round(avg(int16), 5),
    sum(int16),
    min(int8),
    max(int8),
    round(avg(int8), 5),
    sum(int8)
FROM testNullableStates
GROUP BY id
ORDER BY id ASC;

select ' ---- select with states ---- ';

SELECT id, count(),
    minMerge(stringMin),
    maxMerge(stringMax),
    round(minMerge(float64Min),5),
    round(maxMerge(float64Max),5),
    round(avgMerge(float64Avg),5),
    round(sumMerge(float64Sum),5),
    round(minMerge(float32Min),5),
    round(maxMerge(float32Max),5),
    round(avgMerge(float32Avg),5),
    round(sumMerge(float32Sum),5),
    minMerge(decimal325Min),
    maxMerge(decimal325Max),
    round(avgMerge(decimal325Avg), 5),
    sumMerge(decimal325Sum),
    minMerge(dateMin),
    maxMerge(dateMax),
    minMerge(datetimeMin),
    maxMerge(datetimeMax),
    minMerge(datetime64Min),
    maxMerge(datetime64Max),
    minMerge(int64Min),
    maxMerge(int64Max),
    round(avgMerge(int64Avg), 5),
    sumMerge(int64Sum),
    minMerge(int32Min),
    maxMerge(int32Max),
    round(avgMerge(int32Avg), 5),
    sumMerge(int32Sum),
    minMerge(int16Min),
    maxMerge(int16Max),
    round(avgMerge(int16Avg), 5),
    sumMerge(int16Sum),
    minMerge(int8Min),
    maxMerge(int8Max),
    round(avgMerge(int8Avg), 5),
    sumMerge(int8Sum)
FROM testNullableStatesAgg
GROUP BY id
ORDER BY id ASC;


select ' ---- select row with nulls without states ---- ';

SELECT id, count(),
    min(string),
    max(string),
    round(min(float64),5),
    round(max(float64),5),
    round(avg(float64),5),
    round(sum(float64),5),
    round(min(float32),5),
    round(max(float32),5),
    round(avg(float32),5),
    round(sum(float32),5),
    min(decimal325),
    max(decimal325),
    round(avg(decimal325), 5),
    sum(decimal325),
    min(date),
    max(date),
    min(datetime),
    max(datetime),
    min(datetime64),
    max(datetime64),
    min(int64),
    max(int64),
    round(avg(int64), 5),
    sum(int64),
    min(int32),
    max(int32),
    round(avg(int32), 5),
    sum(int32),
    min(int16),
    max(int16),
    round(avg(int16), 5),
    sum(int16),
    min(int8),
    max(int8),
    round(avg(int8), 5),
    sum(int8)
FROM testNullableStates
WHERE id = '-2'
GROUP BY id
ORDER BY id ASC;

select ' ---- select row with nulls with states ---- ';

SELECT id, count(),
    minMerge(stringMin),
    maxMerge(stringMax),
    round(minMerge(float64Min),5),
    round(maxMerge(float64Max),5),
    round(avgMerge(float64Avg),5),
    round(sumMerge(float64Sum),5),
    round(minMerge(float32Min),5),
    round(maxMerge(float32Max),5),
    round(avgMerge(float32Avg),5),
    round(sumMerge(float32Sum),5),
    minMerge(decimal325Min),
    maxMerge(decimal325Max),
    round(avgMerge(decimal325Avg), 5),
    sumMerge(decimal325Sum),
    minMerge(dateMin),
    maxMerge(dateMax),
    minMerge(datetimeMin),
    maxMerge(datetimeMax),
    minMerge(datetime64Min),
    maxMerge(datetime64Max),
    minMerge(int64Min),
    maxMerge(int64Max),
    round(avgMerge(int64Avg), 5),
    sumMerge(int64Sum),
    minMerge(int32Min),
    maxMerge(int32Max),
    round(avgMerge(int32Avg), 5),
    sumMerge(int32Sum),
    minMerge(int16Min),
    maxMerge(int16Max),
    round(avgMerge(int16Avg), 5),
    sumMerge(int16Sum),
    minMerge(int8Min),
    maxMerge(int8Max),
    round(avgMerge(int8Avg), 5),
    sumMerge(int8Sum)
FROM testNullableStatesAgg
WHERE id = '-2'
GROUP BY id
ORDER BY id ASC;


select ' ---- select no rows without states ---- ';

SELECT count(),
    min(string),
    max(string),
    round(min(float64),5),
    round(max(float64),5),
    round(avg(float64),5),
    round(sum(float64),5),
    round(min(float32),5),
    round(max(float32),5),
    round(avg(float32),5),
    round(sum(float32),5),
    min(decimal325),
    max(decimal325),
    round(avg(decimal325), 5),
    sum(decimal325),
    min(date),
    max(date),
    min(datetime),
    max(datetime),
    min(datetime64),
    max(datetime64),
    min(int64),
    max(int64),
    round(avg(int64), 5),
    sum(int64),
    min(int32),
    max(int32),
    round(avg(int32), 5),
    sum(int32),
    min(int16),
    max(int16),
    round(avg(int16), 5),
    sum(int16),
    min(int8),
    max(int8),
    round(avg(int8), 5),
    sum(int8)
FROM testNullableStates
WHERE id = '-22';

select ' ---- select no rows with states ---- ';

SELECT count(),
    minMerge(stringMin),
    maxMerge(stringMax),
    round(minMerge(float64Min),5),
    round(maxMerge(float64Max),5),
    round(avgMerge(float64Avg),5),
    round(sumMerge(float64Sum),5),
    round(minMerge(float32Min),5),
    round(maxMerge(float32Max),5),
    round(avgMerge(float32Avg),5),
    round(sumMerge(float32Sum),5),
    minMerge(decimal325Min),
    maxMerge(decimal325Max),
    round(avgMerge(decimal325Avg), 5),
    sumMerge(decimal325Sum),
    minMerge(dateMin),
    maxMerge(dateMax),
    minMerge(datetimeMin),
    maxMerge(datetimeMax),
    minMerge(datetime64Min),
    maxMerge(datetime64Max),
    minMerge(int64Min),
    maxMerge(int64Max),
    round(avgMerge(int64Avg), 5),
    sumMerge(int64Sum),
    minMerge(int32Min),
    maxMerge(int32Max),
    round(avgMerge(int32Avg), 5),
    sumMerge(int32Sum),
    minMerge(int16Min),
    maxMerge(int16Max),
    round(avgMerge(int16Avg), 5),
    sumMerge(int16Sum),
    minMerge(int8Min),
    maxMerge(int8Max),
    round(avgMerge(int8Avg), 5),
    sumMerge(int8Sum)
FROM testNullableStatesAgg
WHERE id = '-22';

DROP TABLE testNullableStates;
DROP TABLE testNullableStatesAgg;
