SELECT '--- Wrong arguments';
SELECT colorOKLCHToSRGB(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT colorOKLCHToSRGB(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorOKLCHToSRGB((1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorOKLCHToSRGB((1, 'a', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorOKLCHToSRGB((1, 2, 3), 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }


SELECT '--- Regular calls';
WITH colorOKLCHToSRGB((0, 0, 0)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.628, 0.2577, 29.23)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.8664, 0.294827, 142.4953)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.452, 0.313214, 264.052)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.0823, 0.008, 240.75)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.833, 0.264, 144.44)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.5701, 0.194, 293.9)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

SELECT '--- Varying gamma';
WITH colorOKLCHToSRGB((0.4466, 0.0991, 45.44), 1.0) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.4466, 0.0991, 45.44), 1.8) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.4466, 0.0991, 45.44), 2.2) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.4466, 0.0991, 45.44), 2.4) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.4466, 0.0991, 45.44), 3.0) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

SELECT '--- Edge case colors';
WITH colorOKLCHToSRGB((0.6, 0, 0)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((-0.591, 0.1047, 57.35)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.591, 0.1047, 237.35), 0) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.591, 0.1047, 237.35), -1000) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((0.591, 0.1047, 237.35), 1000) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorOKLCHToSRGB((1e3, 1e6, 180)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

SELECT '--- Fuzzing';
WITH colorOKLCHToSRGB((1e3, 1e6, 180), materialize(0.)) as t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
