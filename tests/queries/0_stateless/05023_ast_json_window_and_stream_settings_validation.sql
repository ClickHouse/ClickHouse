-- Window frame direction fields are serialized explicitly. They must be present for
-- offset boundaries and must retain the parser-defined canonical values otherwise.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON($$SELECT count() OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t$$),
    '"frame_begin_preceding":true,',
    '')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON($$SELECT count() OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t$$),
    '"frame_begin_preceding":true',
    '"frame_begin_preceding":false')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON($$SELECT count() OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t$$),
    '"frame_end_preceding":false',
    '"frame_end_preceding":true')); -- { serverError BAD_ARGUMENTS }

-- Stream cursor values use ParserUnsignedInteger and must not deserialize to SQL
-- the parser itself rejects.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON($$SELECT * FROM t STREAM CURSOR {'a': 1}$$),
    '"value":1',
    '"value":-1')); -- { serverError BAD_ARGUMENTS }
