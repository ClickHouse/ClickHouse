SELECT '--- Wrong arguments';
SELECT colorSRGBToOkLCH(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT colorSRGBToOkLCH(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOkLCH((1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOkLCH((1, 'a', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOkLCH((1, 2, 3), 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET allow_experimental_object_type = 1;
DROP FUNCTION IF EXISTS tup_round6;
CREATE FUNCTION tup_round6 AS (t) -> tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

SELECT '--- Regular calls';
SELECT tup_round6(colorSRGBToOkLCH((0, 0, 0)));
SELECT tup_round6(colorSRGBToOkLCH((255, 0, 0)));
SELECT tup_round6(colorSRGBToOkLCH((0, 255, 0)));
SELECT tup_round6(colorSRGBToOkLCH((0, 0, 255)));
SELECT tup_round6(colorSRGBToOkLCH((1, 2, 3))); 
SELECT tup_round6(colorSRGBToOkLCH((15, 241, 63)));
SELECT tup_round6(colorSRGBToOkLCH((129, 87, 220)));

SELECT '--- Varying gamma';
SELECT tup_round6(colorSRGBToOkLCH((128, 64, 32), 1.0));
SELECT tup_round6(colorSRGBToOkLCH((128, 64, 32), 1.8));
SELECT tup_round6(colorSRGBToOkLCH((128, 64, 32), 2.2));
SELECT tup_round6(colorSRGBToOkLCH((128, 64, 32), 2.4));
SELECT tup_round6(colorSRGBToOkLCH((128, 64, 32), 3.0));

SELECT '--- Edge case colors';
SELECT tup_round6(colorSRGBToOkLCH((128, 128, 128)));
SELECT tup_round6(colorSRGBToOkLCH((0.628, 0.2577, 29.2)));
SELECT tup_round6(colorSRGBToOkLCH((-53, -134, -180)));
SELECT tup_round6(colorSRGBToOkLCH((53, 134, 180), 0));
SELECT tup_round6(colorSRGBToOkLCH((53, 134, 180), -1000));
SELECT tup_round6(colorSRGBToOkLCH((53, 134, 180), 1000));
SELECT tup_round6( colorSRGBToOkLCH((1e-3, 1e-6, 180)));