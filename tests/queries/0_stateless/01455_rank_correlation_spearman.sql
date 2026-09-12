-- Tags: no-parallel

CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE IF EXISTS moons;
DROP TABLE IF EXISTS circles;

SELECT '1';
SELECT rankCorr(number, number) FROM numbers(100);

SELECT '-1';
SELECT rankCorr(number, -1 * CAST(number AS Int64)) FROM numbers(100);

SELECT '-0.037';
SELECT roundBankers(rankCorr(exp(number), sin(number)), 3) FROM numbers(100);

CREATE TABLE moons(a Float64, b Float64) Engine=Memory();
INSERT INTO moons VALUES (1.230365,1.291454), (1.93851,0.6499), (1.574085,0.744109), (1.416457,1.41872), (1.90165,1.298199), (2.023844,1.142459), (1.828602,0.636404), (1.568649,1.157387), (1.968863,1.160039), (1.790198,0.860815), (1.238993,0.252486), (1.690338,0.573545), (1.678741,0.739649), (1.363346,0.514698), (1.924442,0.484331), (0.849071,0.585017), (1.859407,1.098124), (1.657176,1.314958), (1.085181,0.761741), (1.184481,0.639135), (1.59856,0.688384), (1.304818,1.212579), (1.913821,0.663551), (1.872619,0.510627), (1.29273,0.795267), (1.767669,0.892397), (1.790311,1.21813), (1.621893,1.229768), (1.525505,0.752643), (1.513535,1.016012), (1.120456,1.427238), (1.71505,0.716654), (1.394756,0.733629), (1.746027,1.422821), (1.5376,1.387397), (1.358968,0.575393), (1.941569,0.572639), (1.904995,0.966926), (1.967455,0.436449), (2.045535,0.582434), (1.365599,0.446582), (2.035874,0.468542), (1.419283,0.739308), (1.718267,0.895579), (1.285871,1.014628), (2.010657,1.631207), (1.78226,0.576882), (1.78274,0.727585), (1.454934,1.285701), (1.657208,0.581418);
SELECT '-0.108';
SELECT roundBankers(rankCorr(a, b), 3) from moons;

CREATE TABLE circles(a Float64, b Float64) Engine=Memory();
INSERT INTO circles VALUES (1.20848,0.505643), (1.577706,1.726383), (1.945215,1.638926), (0.493616,0.792443), (0.827802,1.41133), (1.012179,1.654582), (1.815329,0.254426), (-0.068102,1.456476), (1.235432,1.565291), (1.269633,1.857153), (0.687433,1.24911), (0.131356,1.610389), (1.991372,0.204134), (1.678587,1.456911), (0.501133,0.68513), (0.924535,0.541514), (0.574115,0.340542), (-0.013384,1.17037), (0.917257,1.799431), (1.364786,0.396457), (1.931339,1.093935), (0.575076,0.427512), (2.084798,1.752707), (0.694029,0.257422), (-0.003821,0.160859), (0.037966,0.217695), (1.986527,1.249144), (1.864518,0.521483), (0.038928,0.175741), (1.855737,1.678827), (0.779503,0.963619), (0.035384,0.238397), (0.136108,0.128737), (0.0581,1.093712), (-0.012542,0.713137), (1.53441,0.447265), (0.198885,1.232961), (1.66781,0.259156), (1.478017,1.256315), (1.148358,1.659979), (0.340698,0.76793), (0.376184,0.578202), (0.251495,1.765917), (1.836389,1.75769), (1.573166,1.753943), (0.448309,0.965337), (1.704437,1.138451), (1.93234,1.723736), (1.412218,0.603027), (1.978789,0.938132);
SELECT '0.286';
SELECT roundBankers(rankCorr(a, b), 3) from circles;

-- Tied values get mid-ranks, whose Pearson correlation is the coefficient.
SELECT '-1';
SELECT rankCorr(x, y) FROM values('x Float64, y Float64', (1, 2), (1, 2), (2, 1), (2, 1));

SELECT '0';
SELECT rankCorr(x, y) FROM values('x Float64, y Float64', (1, 1), (1, 2), (2, 1), (2, 2));

SELECT '-0.333';
SELECT roundBankers(rankCorr(x, y), 3) FROM values('x Float64, y Float64', (0, 1), (0, 0), (0, 0), (1, 0));

SELECT '0.861';
SELECT roundBankers(rankCorr(x, y), 3) FROM values('x Float64, y Float64', (1, 1), (1, 2), (1, 3), (2, 3), (2, 4), (3, 5));

-- A constant column has no rank variance, so the correlation is undefined.
SELECT 'nan';
SELECT rankCorr(x, y) FROM values('x Float64, y Float64', (5, 1), (5, 2), (5, 3));

-- A NaN drops its whole row, so trailing and interleaved NaNs both keep the samples paired.
SELECT '-1';
SELECT rankCorr(-number, CAST(if(number < 5, number, nan) AS Float64)) FROM numbers(10);

SELECT '-0.5';
SELECT rankCorr(x, y) FROM values('x Float64, y Float64', (0, 0), (0, nan), (1, 0), (0, 1));

SELECT '0.949';
SELECT roundBankers(rankCorr(x, y), 3) FROM values('x Float64, y Float64', (1, 1), (nan, 5), (1, 2), (2, nan), (2, 3), (3, 9));

-- A state from an older server can hold unequal sizes, and it records no row pairing,
-- so which observations survived is unrecoverable.
SELECT 'nan';
SELECT finalizeAggregation(CAST(unhex('040300000000000000000000000000000000000000000000F03F000000000000000000000000000000000000000000000000000000000000F03F') AS AggregateFunction(rankCorr, Float64, Float64)));

-- Merging concatenates both vectors, so two such states can cancel each other's size skew.
-- Either merge order stays unrecoverable.
SELECT 'nan';
SELECT arrayReduce('rankCorrMerge', [CAST(unhex('01000000000000000000') AS AggregateFunction(rankCorr, Float64, Float64)), CAST(unhex('02030000000000000000000000000000F03F0000000000000000000000000000F03F0000000000000000') AS AggregateFunction(rankCorr, Float64, Float64))]);

SELECT 'nan';
SELECT arrayReduce('rankCorrMerge', [CAST(unhex('02030000000000000000000000000000F03F0000000000000000000000000000F03F0000000000000000') AS AggregateFunction(rankCorr, Float64, Float64)), CAST(unhex('01000000000000000000') AS AggregateFunction(rankCorr, Float64, Float64))]);

-- An older state with equal sizes was never unpaired, so it still computes.
SELECT '0.5';
SELECT finalizeAggregation(CAST(unhex('03030000000000000000000000000000F03F000000000000004000000000000000000000000000000040000000000000F03F') AS AggregateFunction(rankCorr, Float64, Float64)));

DROP TABLE IF EXISTS moons;
DROP TABLE IF EXISTS circles;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};


