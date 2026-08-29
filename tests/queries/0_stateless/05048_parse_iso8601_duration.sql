-- Examples from the feature request.
SELECT parseISO8601Duration('PT1M');
SELECT parseISO8601Duration('PT1H30M');
SELECT parseISO8601Duration('P1DT12H');
SELECT parseISO8601Duration('PT1.5S');
SELECT parseISO8601Duration('P2W');

-- All designators together, and a fractional tail.
SELECT parseISO8601Duration('P1W2DT3H4M5S');
SELECT parseISO8601Duration('P1DT12H30M5.5S');

-- A fraction is accepted on any component, not only on seconds.
SELECT parseISO8601Duration('PT0.5H');

-- Zero is a valid duration.
SELECT parseISO8601Duration('PT0S');

-- The return type is Float64 regardless of whether the result is integral.
SELECT toTypeName(parseISO8601Duration('PT1M'));

SELECT parseISO8601Duration(materialize('PT1H30M'));

-- Years and months are rejected: neither has a fixed length in seconds.
SELECT parseISO8601Duration('P1Y'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('P1M'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('P3Y6M4DT12H30M5S'); -- { serverError BAD_ARGUMENTS }

-- Structural errors.
SELECT parseISO8601Duration(''); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('1D'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('P'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('PT'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('P1.5'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('P1.D'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('PT1X'); -- { serverError BAD_ARGUMENTS }

-- Designators must be in canonical order and appear at most once.
SELECT parseISO8601Duration('PT1S1S'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('PT1M1H'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('P1D2W'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('PT1HT1M'); -- { serverError BAD_ARGUMENTS }

-- Time designators require T, and date designators must precede it.
SELECT parseISO8601Duration('P1H'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('PT1D'); -- { serverError BAD_ARGUMENTS }

-- Wrong argument type and arity.
SELECT parseISO8601Duration(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT parseISO8601Duration(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT parseISO8601Duration('PT1M', 'PT1M'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
