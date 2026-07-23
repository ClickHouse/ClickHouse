SET enable_analyzer = 1;

SELECT toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, toDate('2000-01-02') AS y, x > y ? x : y AS z;
SELECT materialize(toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, toDate('2000-01-02') AS y, x > y ? x : y AS z;
SELECT toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, materialize(toDate('2000-01-02')) AS y, x > y ? x : y AS z;
SELECT materialize(toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, materialize(toDate('2000-01-02')) AS y, x > y ? x : y AS z;

SELECT toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, toDate('2000-01-02') AS y, 0 ? x : y AS z;
SELECT materialize(toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, toDate('2000-01-02') AS y, 0 ? x : y AS z;
SELECT toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, materialize(toDate('2000-01-02')) AS y, 0 ? x : y AS z;
SELECT materialize(toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, materialize(toDate('2000-01-02')) AS y, 0 ? x : y AS z;

SELECT toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, toDate('2000-01-02') AS y, 1 ? x : y AS z;
SELECT materialize(toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, toDate('2000-01-02') AS y, 1 ? x : y AS z;
SELECT toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, materialize(toDate('2000-01-02')) AS y, 1 ? x : y AS z;
SELECT materialize(toDateTime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, materialize(toDate('2000-01-02')) AS y, 1 ? x : y AS z;

SELECT rand() % 2 = 0 ? number : number FROM numbers(5);

SELECT rand() % 2 = 0 ? number : toString(number) FROM numbers(5); -- { serverError NO_COMMON_TYPE }
