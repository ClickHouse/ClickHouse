SELECT '--- Wrong arguments';
SELECT colorSRGBToOKLCH(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT colorSRGBToOKLCH(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOKLCH((1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOKLCH((1, 'a', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOKLCH((1, 2, 3), 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP FUNCTION IF EXISTS 03561_tup_round6;
CREATE FUNCTION 03561_tup_round6 AS (t) -> tuple(round(t.1, 6), round(t.2, 6), round(t.3, 6));

SELECT '--- Regular calls';
SELECT 03561_tup_round6(colorSRGBToOKLCH((0, 0, 0)));
SELECT 03561_tup_round6(colorSRGBToOKLCH((255, 0, 0)));
SELECT 03561_tup_round6(colorSRGBToOKLCH((0, 255, 0)));
SELECT 03561_tup_round6(colorSRGBToOKLCH((0, 0, 255)));
SELECT 03561_tup_round6(colorSRGBToOKLCH((1, 2, 3))); 
SELECT 03561_tup_round6(colorSRGBToOKLCH((15, 241, 63)));
SELECT 03561_tup_round6(colorSRGBToOKLCH((129, 87, 220)));

SELECT '--- Varying gamma';
SELECT 03561_tup_round6(colorSRGBToOKLCH((128, 64, 32), 1.0));
SELECT 03561_tup_round6(colorSRGBToOKLCH((128, 64, 32), 1.8));
SELECT 03561_tup_round6(colorSRGBToOKLCH((128, 64, 32), 2.2));
SELECT 03561_tup_round6(colorSRGBToOKLCH((128, 64, 32), 2.4));
SELECT 03561_tup_round6(colorSRGBToOKLCH((128, 64, 32), 3.0));

SELECT '--- Edge case colors';
SELECT 03561_tup_round6(colorSRGBToOKLCH((128, 128, 128)));
SELECT 03561_tup_round6(colorSRGBToOKLCH((0.628, 0.2577, 29.2)));
SELECT 03561_tup_round6(colorSRGBToOKLCH((-53, -134, -180)));
SELECT 03561_tup_round6(colorSRGBToOKLCH((53, 134, 180), 0));
SELECT 03561_tup_round6(colorSRGBToOKLCH((53, 134, 180), -1000));
SELECT 03561_tup_round6(colorSRGBToOKLCH((53, 134, 180), 1000));
SELECT 03561_tup_round6( colorSRGBToOKLCH((1e-3, 1e-6, 180)));