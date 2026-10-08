-- Tags: no-darwin
-- no-darwin: the imprecise float parser (precise_float_parsing = 0, and visitParam*) rounds to different least-significant digits on Darwin.

-- JSONExtract parses a quoted numeric string as Float64 and must honor precise_float_parsing.
SELECT 'JSONExtract precise (default)';
SELECT JSONExtract('{"x":"1.6725"}', 'x', 'Float64');
SELECT 'JSONExtract fast';
SELECT JSONExtract('{"x":"1.6725"}', 'x', 'Float64') SETTINGS precise_float_parsing = 0;

-- The Float64 target of JSONExtract for a non-string number is taken from the parser's double directly,
-- so it is precise regardless of the setting (kept here as documentation of the behavior).
SELECT 'JSONExtract number';
SELECT JSONExtract('{"x":1.6725}', 'x', 'Float64');

-- visitParamExtractFloat / simpleJSONExtractFloat have no access to the setting and stay on the
-- imprecise parser (their pre-26.7 behavior), independent of precise_float_parsing.
SELECT 'visitParamExtractFloat (always imprecise)';
SELECT visitParamExtractFloat('{"x":1.6725}', 'x');
SELECT visitParamExtractFloat('{"x":1.6725}', 'x') SETTINGS precise_float_parsing = 1;
