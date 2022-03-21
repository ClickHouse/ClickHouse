-- Tags: no-fasttest

SELECT h3EdgeLengthM(100); -- { serverError 69 }
SELECT h3HexAreaM2(100); -- { serverError 69 }
SELECT h3HexAreaKm2(100); -- { serverError 69 }
