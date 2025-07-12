SELECT '--- Wrong arguments';
SELECT colorSRGBToOkLCH(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT colorSRGBToOkLCH(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOkLCH((1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOkLCH((1, 'a', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT colorSRGBToOkLCH((1, 2, 3), 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- Regular calls';
SELECT colorSRGBToOkLCH((0, 0, 0));
SELECT colorSRGBToOkLCH((255, 0, 0));
SELECT colorSRGBToOkLCH((0, 255, 0));
SELECT colorSRGBToOkLCH((0, 0, 255));
SELECT colorSRGBToOkLCH((1, 2, 3)); 
SELECT colorSRGBToOkLCH((15, 241, 63)); 
SELECT colorSRGBToOkLCH((129, 87, 220)); 

SELECT '--- Varying gamma';
SELECT colorSRGBToOkLCH((128, 64, 32), 1.0);
SELECT colorSRGBToOkLCH((128, 64, 32), 2.4);
SELECT colorSRGBToOkLCH((128, 64, 32), 3.0);

SELECT '--- Edge case colors';
SELECT colorSRGBToOkLCH((128, 128, 128));
SELECT colorSRGBToOkLCH((-53, -134, -180));
SELECT colorSRGBToOkLCH((53, 134, 180), 0);
SELECT colorSRGBToOkLCH((53, 134, 180), -1000);
SELECT colorSRGBToOkLCH((53, 134, 180), 1000);
SELECT colorSRGBToOkLCH((-1e3, -1e6, 180));
