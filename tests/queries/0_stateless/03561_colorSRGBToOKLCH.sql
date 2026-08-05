SELECT '--- Wrong arguments';
SELECT colorSRGBToOKLCH(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT colorSRGBToOKLCH(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOKLCH((1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOKLCH((1, 'a', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOKLCH((1, 2, 3), 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- Regular calls';
WITH colorSRGBToOKLCH((0, 0, 0)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((255, 0, 0)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((0, 255, 0)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((0, 0, 255)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((1, 2, 3)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((15, 241, 63)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((129, 87, 220)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

SELECT '--- Varying gamma';
WITH colorSRGBToOKLCH((128, 64, 32), 1.0) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((128, 64, 32), 1.8) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((128, 64, 32), 2.2) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((128, 64, 32), 2.4) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((128, 64, 32), 3.0) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

SELECT '--- Edge case colors';
WITH colorSRGBToOKLCH((128, 128, 128)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((0.628, 0.2577, 29.2)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((-53, -134, -180)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((53, 134, 180), 0) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((53, 134, 180), -1000) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH colorSRGBToOKLCH((53, 134, 180), 1000) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
WITH  colorSRGBToOKLCH((1e-3, 1e-6, 180)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

SELECT '--- Fuzzing';
WITH colorSRGBToOKLCH((128, 128, 128), materialize(1.)) AS t
SELECT tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));
