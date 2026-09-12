-- Boundary-equality edge case
select histogramExplicit([10])(10);
select histogramExplicitOpenClosed([10])(10);

-- Unsorted boundaries and unsorted duplicates
select histogramExplicit([20, 0, 10])(number) from numbers(5);
select histogramExplicitOpenClosed([0, 10, 10, 20])(number) from numbers(5);
select histogramExplicit([20, 10, 0, 10])(number) from numbers(5);

-- Boundaries with explicit inf / -inf
select histogramExplicit([inf, 0, 10, 10, 20, -inf])(number) from numbers(5);

SELECT histogramExplicit([-inf, 0, 10, 20, inf])(number) 
FROM (SELECT number - 2 AS number FROM numbers(5));

SELECT histogramExplicitOpenClosed([-inf, 0, 10, 20, inf])(number) 
FROM (SELECT number - 2 AS number FROM numbers(5));

select histogramExplicit([-inf, inf])(number) from numbers(4);
select histogramExplicit([-inf])(number) from numbers(3);
select histogramExplicitOpenClosed([inf])(number) from numbers(2);

-- +-inf value behavior
SELECT histogramExplicit([-10, 0, 10])(toFloat64(number_with_inf))
FROM (SELECT arrayJoin([-inf, -5.0, 0.0, 5.0, inf, nan]) AS number_with_inf);

SELECT histogramExplicitOpenClosed([-10, 0, 10])(toFloat64(number_with_inf))
FROM (SELECT arrayJoin([-inf, -5.0, 0.0, 5.0, inf, nan]) AS number_with_inf);

-- MAX_BOUNDARIES (250)
select length(tupleElement(histogramExplicit([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250])(number), 1)) from (select * from system.numbers limit 1);

-- Unsorted input values
select histogramExplicit([0, 10, 20])(value)
from (select arrayJoin([19, 3, 15, 7, 10, 1, 18, 5, 12, 0]) as value);

select histogramExplicitOpenClosed([0, 10, 20])(value)
from (select arrayJoin([19, 3, 15, 7, 10, 1, 18, 5, 12, 0]) as value);

-- Special values ('null', 'nan', 'zero') without tables
SELECT histogramExplicit([0, 10], 'null', 'nan', 'zero')(x)
FROM (SELECT arrayJoin([cast(NULL, 'Nullable(Float64)'), NULL, 0., 0., 5., -3., 0./0.]) AS x);

SELECT histogramExplicit([0, 10], 'null', 'nan')(x)
FROM (SELECT arrayJoin([cast(NULL, 'Nullable(Float64)'), NULL, 0., 0., 5., -3., 0./0.]) AS x);

SELECT histogramExplicit([0, 10])(x)
FROM (SELECT arrayJoin([cast(NULL, 'Nullable(Float64)'), NULL, 0., 0., 5., -3., 0./0.]) AS x);

-- Only NULLs when 'null' is NOT requested (must be safe / empty result)
SELECT histogramExplicit([0, 10])(cast(NULL, 'Nullable(Float64)'));

-- Combinators: -If, -Array
select histogramExplicitIf([3, 6])(number, number > 5) from (select * from system.numbers limit 10);
SELECT histogramExplicitArray([0, 10, 20])([1, 5, 12, 25]);

-- State & Merge (ClosedOpen & OpenClosed)
SELECT finalizeAggregation(
    (SELECT histogramExplicitState([0, 50, 100])(number) FROM numbers(50, 100))
    +
    (SELECT histogramExplicitState([0, 50, 100])(number) FROM numbers(0, 100))
);

SELECT finalizeAggregation(
    (SELECT histogramExplicitOpenClosedState([0, 50, 100])(number) FROM numbers(50, 100))
    +
    (SELECT histogramExplicitOpenClosedState([0, 50, 100])(number) FROM numbers(0, 100))
);

SELECT finalizeAggregation(
    (SELECT histogramExplicitState([0, 50, 100], 'zero', 'null', 'nan')(value)
     FROM (SELECT arrayJoin([NULL, toNullable(0.0), cast('nan', 'Nullable(Float64)'), cast('nan', 'Nullable(Float64)'), toNullable(-100.0), toNullable(25.0), toNullable(75.0), cast('inf', 'Nullable(Float64)')]) AS value))
    +
    (SELECT histogramExplicitState([0, 50, 100], 'zero', 'null', 'nan')(value)
     FROM (SELECT arrayJoin([NULL, NULL, toNullable(-0.0), toNullable(0.0), cast('nan', 'Nullable(Float64)'), cast('-inf', 'Nullable(Float64)'), toNullable(50.0), toNullable(100.0), toNullable(150.0)]) AS value))
);

SELECT k, histogramExplicit([0], 'null')(x)
FROM values('k UInt8, x Nullable(Int32)', (1, NULL), (1, 1))
GROUP BY k ORDER BY k;

SELECT histogramExplicit([0], 'null')(x)
FROM values('x Nullable(Int32)', (NULL), (NULL));

SELECT histogramExplicitIf([0], 'null')(x, cond)
FROM values('x Nullable(Int32), cond UInt8', (NULL, 1), (1, 0));

SELECT histogramExplicitIf([0], 'null')(x, cond) 
FROM values('x Int32, cond Nullable(UInt8)', (-1, 1), (1, 1), (2, NULL), (3, 0));

SELECT k, histogramExplicitIf([0], 'null')(x, cond)
FROM values('k UInt8, x Nullable(Int32), cond UInt8', (1, NULL, 1), (1, -1, 1), (1, 5, 0), (2, NULL, 0), (2, 5, 1))
GROUP BY k ORDER BY k; 

-- Combinators: -If, -Array, -ArrayIf (Complex Adapter Tests)
-- ArrayIf: Filtering array elements using UInt8 condition
SELECT histogramExplicitArray([0, 10, 20])(arrayFilter((x, m) -> m, [1, 5, 12, 25], [1, 0, 1, 1]));

-- ArrayIf + Nullable values + Special flags:
SELECT 
    histogramExplicitArray([0, 10], 'null', 'nan', 'zero')(
        arrayFilter(
            (x, mask) -> mask,
            cast([NULL, 0.0, 5.0, cast('nan', 'Nullable(Float64)'), 7.0], 'Array(Nullable(Float64))'),
            [1, 0, 1, 0, 0]
        )
    );

-- ArrayIf with a fully Nullable row-level condition column (cond Nullable(UInt8)):
SELECT histogramExplicitArrayIf([0, 10], 'null')(arr, cond)
FROM values(
    'arr Array(Nullable(Int32)), cond UInt8',
    ([1, 5, NULL], 1),
    ([2, 10, 15], 0)
);

SELECT 
    k, 
    histogramExplicitArrayIf([0, 10], 'null')(x, cond)
FROM values(
    'k UInt8, x Array(Nullable(Int32)), cond Nullable(UInt8)',
    (1, [1, NULL], 1),
    (1, [2, NULL], 0),
    (1, [3, NULL], NULL)
)
GROUP BY k ORDER BY k;

--regression on merging states
SELECT
    k,
    finalizeAggregation(
    histogramExplicitStateIf([0], 'null')(x, cond)) AS state_res
    --,histogramExplicitIf([0], 'null')(x, cond) AS direct_res
FROM
    values('k UInt8, x Nullable(Int32), cond Nullable(UInt8)', 
            (1, null, 1), 
            (1, -1, 1), 
            (1, 5, 0), 
            (2, null, 0),
            (2, 5, 1))
GROUP BY k
ORDER BY k;

SELECT finalizeAggregation(
    (
        SELECT histogramExplicitState([20, 0, 10], 'null')(val)
        FROM (
            SELECT cast(5.0, 'Nullable(Float64)') AS val
            UNION ALL SELECT cast(15.0, 'Nullable(Float64)')
            UNION ALL SELECT cast(null, 'Nullable(Float64)')
        )
    )
    +
    (
        SELECT histogramExplicitState([0, 10, 20], 'null')(val)
        FROM (
            SELECT cast(25.0, 'Nullable(Float64)') AS val
            UNION ALL SELECT cast(null, 'Nullable(Float64)')
        )
    )
) AS merged_res;

SELECT finalizeAggregation(
    CAST(
        (SELECT histogramExplicitState([20, 0, 10], 'null')(cast(5.0, 'Nullable(Float64)'))),
        'AggregateFunction(histogramExplicit([0, 10, 20], \'null\'), Nullable(Float64))'
    )
) AS cast_res;

SELECT finalizeAggregation(
    (SELECT histogramExplicitState([0, 50, 100], 'zero', 'null', 'nan')(value)
     FROM (SELECT arrayJoin([NULL, toNullable(0.0), cast('nan', 'Nullable(Float64)'), cast('nan', 'Nullable(Float64)'), toNullable(-100.0), toNullable(25.0), toNullable(75.0), cast('inf', 'Nullable(Float64)')]) AS value))
    +
    (SELECT histogramExplicitState([50, 0, 100], 'zero', 'null', 'nan')(value)
     FROM (SELECT arrayJoin([NULL, NULL, toNullable(-0.0), toNullable(0.0), cast('nan', 'Nullable(Float64)'), cast('-inf', 'Nullable(Float64)'), toNullable(50.0), toNullable(100.0), toNullable(150.0)]) AS value))
);

SELECT finalizeAggregation(
    (SELECT histogramExplicitState([0, 50, 100])(value)
     FROM (SELECT arrayJoin([NULL, toNullable(0.0), cast('nan', 'Nullable(Float64)'), cast('nan', 'Nullable(Float64)'), toNullable(-100.0), toNullable(25.0), toNullable(75.0), cast('inf', 'Nullable(Float64)')]) AS value))
    +
    (SELECT histogramExplicitState([50, 0, 100])(value)
     FROM (SELECT arrayJoin([NULL, NULL, toNullable(-0.0), toNullable(0.0), cast('nan', 'Nullable(Float64)'), cast('-inf', 'Nullable(Float64)'), toNullable(50.0), toNullable(100.0), toNullable(150.0)]) AS value))
);

SELECT finalizeAggregation(
    (SELECT histogramExplicitState([0, 50, 100])(assumeNotNull(value))
     FROM (SELECT arrayJoin([0.0, -100.0, 25.0, 75.0]) AS value))
    +
    (SELECT histogramExplicitState([50, 0, 100])(assumeNotNull(value))
     FROM (SELECT arrayJoin([-0.0, 0.0, 50.0, 100.0, 150.0]) AS value))
);

SELECT finalizeAggregation(
    (SELECT histogramExplicitStateIf([0, 50, 100], 'zero', 'null', 'nan')(value, value > 3)
     FROM (SELECT arrayJoin([NULL, toNullable(0.0), cast('nan', 'Nullable(Float64)'), cast('nan', 'Nullable(Float64)'), toNullable(-100.0), toNullable(25.0), toNullable(75.0), cast('inf', 'Nullable(Float64)')]) AS value))
    +
    (SELECT histogramExplicitStateIf([50, 0, 100], 'zero', 'null', 'nan')(value, value > 3)
     FROM (SELECT arrayJoin([NULL, NULL, toNullable(-0.0), toNullable(0.0), cast('nan', 'Nullable(Float64)'), cast('-inf', 'Nullable(Float64)'), toNullable(50.0), toNullable(100.0), toNullable(150.0)]) AS value))
);