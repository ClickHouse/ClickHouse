-- `Poco::JSON` reads integers with `Poco::NumberParser`, whose `strToInt` accumulated digits without
-- a complete overflow check. A number that does not fit into `Int64` overflowed the accumulator
-- (undefined behavior, reported by `UndefinedBehaviorSanitizer` in the AST fuzzer) and the wrapped
-- value was returned as a successfully parsed number, so `18446744073709551617` became `1`.

-- Out of range for `UInt64`: must be rejected, not wrapped around.
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"UInt64","value":18446744073709551617}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"UInt64","value":36893488147419103233}}'); -- { serverError BAD_ARGUMENTS }

-- A value above `Int64` maximum is a valid `UInt64` and must round trip.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 9223372036854775807, 9223372036854775808, 9223372036854775809, 18446744073709551615'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT -9223372036854775807, -1, 0, 1'));
