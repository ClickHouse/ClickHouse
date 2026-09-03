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

-- `LowCardinality` and `Nullable` arguments reach the function through the default implementations,
-- which apply it to the dictionary and to the values sitting under the null map. Neither of those
-- positions holds a valid duration, so they must not make the function throw.
SELECT parseISO8601Duration(x) AS s
FROM (SELECT CAST(arrayJoin(['PT1S', 'PT2S']) AS LowCardinality(String)) AS x)
ORDER BY s;

SELECT parseISO8601Duration(x) AS s
FROM (SELECT arrayJoin(CAST([NULL, 'PT1S'] AS Array(Nullable(String)))) AS x)
ORDER BY s NULLS LAST;

-- A null argument yields null instead of an attempt to parse whatever sits under the null map.
SELECT parseISO8601Duration(NULL);
SELECT parseISO8601Duration(CAST(NULL AS Nullable(String)));
SELECT toTypeName(parseISO8601Duration(CAST('PT1S' AS Nullable(String))));

-- `Dynamic` and `Variant` arguments go through their own adaptors, which default to
-- `useDefaultImplementationForNulls` and so would have been switched off along with it.
SELECT parseISO8601Duration(CAST('PT1H30M' AS Dynamic)) = parseISO8601Duration('PT1H30M');
SELECT parseISO8601Duration(CAST('PT1H30M' AS Variant(String, UInt8))) = parseISO8601Duration('PT1H30M');

-- ISO 8601 also allows a comma as the decimal separator, and RFC 3339 / XSD add signed durations.
-- Neither is accepted: ClickHouse rejects comma decimals elsewhere, and a sign is not part of the
-- core grammar.
SELECT parseISO8601Duration('PT1,5S'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('P1,5D'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('-PT1S'); -- { serverError BAD_ARGUMENTS }
SELECT parseISO8601Duration('+PT1S'); -- { serverError BAD_ARGUMENTS }

-- Values that do not fit into Float64 are rejected rather than returned as infinity.
-- A single component that overflows on its own:
SELECT parseISO8601Duration(concat('PT', repeat('9', 309), 'S')); -- { serverError BAD_ARGUMENTS }
-- and one that is finite on its own but overflows once scaled to seconds:
SELECT parseISO8601Duration(concat('P', repeat('9', 305), 'D')); -- { serverError BAD_ARGUMENTS }

-- Wrong argument type and arity.
SELECT parseISO8601Duration(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT parseISO8601Duration(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT parseISO8601Duration('PT1M', 'PT1M'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
