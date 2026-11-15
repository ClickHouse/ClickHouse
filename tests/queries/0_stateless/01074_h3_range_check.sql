-- Tags: no-fasttest

SELECT h3EdgeLengthM(100); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT h3HexAreaM2(100); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT h3HexAreaKm2(100); -- { serverError ARGUMENT_OUT_OF_BOUND }
