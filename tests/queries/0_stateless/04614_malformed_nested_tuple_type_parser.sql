-- A malformed nested `Tuple` type name must be reported as a syntax error, not burn the whole
-- parser backtracking budget. `ParserDataType` used to parse the argument list of a failing
-- `Tuple(...)` twice - once by the fast path that builds `ASTTupleDataType`, and once again by the
-- generic argument parser - so the cost of a malformed type of depth N was 2^N. At depth 20 that
-- already exhausted `max_parser_backtracks` and reported `TOO_SLOW_PARSING`.

SELECT defaultValueOfTypeName('Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Int8'); -- { serverError SYNTAX_ERROR }
SELECT defaultValueOfTypeName('Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Int8'); -- { serverError SYNTAX_ERROR }
SELECT defaultValueOfTypeName('Nested(a Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Int8'); -- { serverError SYNTAX_ERROR }

-- A well-formed type of the same depth keeps working.
SELECT toTypeName(defaultValueOfTypeName('Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Int8))))))))))))))))))))))))')) = 'Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Int8))))))))))))))))))))))))';

-- The generic argument parser is still reached for an empty argument list, which the fast path rejects.
SELECT toTypeName(defaultValueOfTypeName('Tuple()')) = 'Tuple()';
SELECT toTypeName(defaultValueOfTypeName('Tuple(UInt8, String)')) = 'Tuple(UInt8, String)';
SELECT tupleElement(defaultValueOfTypeName('Tuple(a UInt8, b String)'), 'b') = '';
SELECT toTypeName(defaultValueOfTypeName('Tuple(UInt8,)')) = 'Tuple(UInt8)';
SELECT toTypeName(defaultValueOfTypeName('Nested(a UInt8, b Nested(c String))')) = 'Nested(a UInt8, b Nested(c String))';
