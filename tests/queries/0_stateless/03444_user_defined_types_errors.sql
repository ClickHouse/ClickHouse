-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.
DROP TYPE IF EXISTS TestType;

CREATE TYPE TestType AS InvalidType; -- { serverError UNKNOWN_TYPE }
CREATE TYPE AS String; -- { clientError SYNTAX_ERROR }

CREATE TYPE TypeA AS TypeB; -- { serverError UNKNOWN_TYPE }
CREATE TYPE SelfRef AS Array(SelfRef); -- { serverError UNKNOWN_TYPE }

-- Only the names that can be used where a data type is expected are accepted.
CREATE TYPE `Type-With-Dashes` AS String; -- { clientError SYNTAX_ERROR }
CREATE TYPE `тип_utf8` AS String; -- { clientError SYNTAX_ERROR }
CREATE TYPE `Null` AS String; -- { clientError SYNTAX_ERROR }
CREATE TYPE `8Bit` AS String; -- { clientError SYNTAX_ERROR }
CREATE TYPE WithBadParameter(`T-1`) AS Array(`T-1`); -- { clientError SYNTAX_ERROR }

CREATE TYPE `Quoted_Type$1` AS String;
SHOW TYPE `Quoted_Type$1`;
SELECT CAST('x' AS `Quoted_Type$1`), toTypeName(CAST('x' AS Quoted_Type$1));
DROP TYPE `Quoted_Type$1`;

CREATE TYPE NestedComplex AS Array(Tuple(Map(String, Array(Tuple(String, UInt64))), Nullable(String)));
SHOW TYPE NestedComplex;
DROP TYPE NestedComplex;

CREATE TYPE CaseSensitive AS String;
SHOW TYPE CaseSensitive;
SHOW TYPE casesensitive; -- { serverError UNKNOWN_TYPE }
DROP TYPE CaseSensitive;
