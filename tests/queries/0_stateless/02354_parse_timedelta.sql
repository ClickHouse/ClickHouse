SELECT parseTimeDelta('1 min 35 sec');
SELECT parseTimeDelta('0m;11.23s.');
SELECT parseTimeDelta('11hr 25min 3.1s');
SELECT parseTimeDelta('0.00123 seconds');
SELECT parseTimeDelta('1yr2mo');
SELECT parseTimeDelta('11s+22min');
SELECT parseTimeDelta('1yr-2mo-4w + 12 days, 3 hours : 1 minute ; 33 seconds');
SELECT parseTimeDelta('1s1ms1us1ns');
SELECT parseTimeDelta('1s1ms1μs1ns'); // μs U+03BC
SELECT parseTimeDelta('1s1ms1µs1ns'); // µs U+00B5
SELECT parseTimeDelta('1s - 1ms : 1μs ; 1ns');
SELECT parseTimeDelta('1.11s1.11ms1.11us1.11ns');

-- invalid expressions
SELECT parseTimeDelta(); -- {serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION}
SELECT parseTimeDelta('1yr', 1); -- {serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION}
SELECT parseTimeDelta(1); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT parseTimeDelta(' '); -- {serverError BAD_ARGUMENTS}
SELECT parseTimeDelta('-1yr'); -- {serverError BAD_ARGUMENTS}
SELECT parseTimeDelta('1yr-'); -- {serverError BAD_ARGUMENTS}
SELECT parseTimeDelta('yr2mo'); -- {serverError BAD_ARGUMENTS}
SELECT parseTimeDelta('1.yr2mo'); -- {serverError BAD_ARGUMENTS}
SELECT parseTimeDelta('1-yr'); -- {serverError BAD_ARGUMENTS}
SELECT parseTimeDelta('1 1yr'); -- {serverError BAD_ARGUMENTS}
SELECT parseTimeDelta('1yyr'); -- {serverError BAD_ARGUMENTS}
SELECT parseTimeDelta('1yr-2mo-4w + 12 days, 3 hours : 1 minute ;. 33 seconds'); -- {serverError BAD_ARGUMENTS}

-- parseTimeDeltaOrNull / parseTimeDeltaOrZero: unparseable input yields NULL / 0 instead of throwing
SELECT s, parseTimeDeltaOrNull(s) FROM values('s String', ('11s+22min'), ('1h 30m'), (''));
SELECT s, parseTimeDeltaOrZero(s) FROM values('s String', ('11s+22min'), ('1h 30m'), (''));

-- a NULL row stays NULL for both wrappers, because the engine re-applies the null map
SELECT s, parseTimeDeltaOrNull(s), parseTimeDeltaOrZero(s) FROM values('s Nullable(String)', ('11s'), (NULL), ('junk'));

SELECT toTypeName(parseTimeDelta('1s')), toTypeName(parseTimeDeltaOrNull('1s')), toTypeName(parseTimeDeltaOrZero('1s')), parseTimeDelta('1s'), parseTimeDeltaOrNull('1s'), parseTimeDeltaOrZero('1s');
SELECT toTypeName(parseTimeDeltaOrNull(toLowCardinality('1s'))), toTypeName(parseTimeDeltaOrZero(toLowCardinality('1s'))), parseTimeDeltaOrNull(toLowCardinality('1s')), parseTimeDeltaOrZero(toLowCardinality('1s'));

-- errors about the call itself still raise for the wrappers
SELECT parseTimeDeltaOrNull(); -- {serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION}
SELECT parseTimeDeltaOrZero(); -- {serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION}
SELECT parseTimeDeltaOrNull('1yr', 1); -- {serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION}
SELECT parseTimeDeltaOrZero('1yr', 1); -- {serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION}
SELECT parseTimeDeltaOrNull(1); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT parseTimeDeltaOrZero(1); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

-- over a Dynamic column an unparseable String alternative is recovered, ...
SELECT dynamicType(d), parseTimeDeltaOrNull(d), parseTimeDeltaOrZero(d) FROM (SELECT materialize(CAST('junk' AS Dynamic)) AS d);
SELECT toTypeName(parseTimeDelta(d)), toTypeName(parseTimeDeltaOrNull(d)), toTypeName(parseTimeDeltaOrZero(d)), parseTimeDelta(d), parseTimeDeltaOrNull(d), parseTimeDeltaOrZero(d) FROM (SELECT materialize(CAST('1s' AS Dynamic)) AS d);
-- ... while a non-String alternative is still a call error, as for the bare function
SELECT parseTimeDeltaOrNull(d) FROM (SELECT materialize(CAST(42 AS Dynamic)) AS d) SETTINGS dynamic_throw_on_type_mismatch = 1; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT parseTimeDeltaOrZero(d) FROM (SELECT materialize(CAST(42 AS Dynamic)) AS d) SETTINGS dynamic_throw_on_type_mismatch = 1; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
-- with dynamic_throw_on_type_mismatch = 0 the adaptor recovers that mismatch to NULL instead;
-- the bare function is selected alongside so the "same as the bare function" claim is asserted
SELECT parseTimeDelta(d), parseTimeDeltaOrNull(d) FROM (SELECT materialize(CAST(42 AS Dynamic)) AS d) SETTINGS dynamic_throw_on_type_mismatch = 0;
SELECT parseTimeDelta(d), parseTimeDeltaOrZero(d) FROM (SELECT materialize(CAST(42 AS Dynamic)) AS d) SETTINGS dynamic_throw_on_type_mismatch = 0;

-- A Variant column goes through a different adaptor than Dynamic, so it gets its own arms.
-- A valid String alternative parses the same for the wrappers as for the bare function
SELECT variantType(v), parseTimeDelta(v), parseTimeDeltaOrNull(v), parseTimeDeltaOrZero(v) FROM (SELECT materialize(CAST('1h 30m' AS Variant(String, UInt64))) AS v);
-- an unparseable one is recovered, while the bare function on the same row throws
SELECT variantType(v), parseTimeDeltaOrNull(v), parseTimeDeltaOrZero(v) FROM (SELECT materialize(CAST('junk' AS Variant(String, UInt64))) AS v);
SELECT parseTimeDelta(v) FROM (SELECT materialize(CAST('junk' AS Variant(String, UInt64))) AS v); -- {serverError BAD_ARGUMENTS}
-- ... and a non-String alternative is still a call error, as for Dynamic
SELECT parseTimeDeltaOrNull(v) FROM (SELECT materialize(CAST(toUInt64(42) AS Variant(String, UInt64))) AS v) SETTINGS variant_throw_on_type_mismatch = 1; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT parseTimeDeltaOrZero(v) FROM (SELECT materialize(CAST(toUInt64(42) AS Variant(String, UInt64))) AS v) SETTINGS variant_throw_on_type_mismatch = 1; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
-- with variant_throw_on_type_mismatch = 0 that mismatch is recovered to NULL, same as the bare function
SELECT parseTimeDelta(v), parseTimeDeltaOrNull(v) FROM (SELECT materialize(CAST(toUInt64(42) AS Variant(String, UInt64))) AS v) SETTINGS variant_throw_on_type_mismatch = 0;
SELECT parseTimeDelta(v), parseTimeDeltaOrZero(v) FROM (SELECT materialize(CAST(toUInt64(42) AS Variant(String, UInt64))) AS v) SETTINGS variant_throw_on_type_mismatch = 0;
