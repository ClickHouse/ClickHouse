DROP TYPE IF EXISTS TestType;

CREATE TYPE TestType AS InvalidType; -- { serverError UNKNOWN_TYPE }
CREATE TYPE AS String; -- { clientError SYNTAX_ERROR }

CREATE TYPE TypeA AS TypeB; -- { serverError UNKNOWN_TYPE }
CREATE TYPE SelfRef AS Array(SelfRef); -- { serverError UNKNOWN_TYPE }

CREATE TYPE `Type-With-Dashes` AS String;
SHOW TYPE `Type-With-Dashes`;
DROP TYPE `Type-With-Dashes`;

CREATE TYPE `тип_utf8` AS String;
SHOW TYPE `тип_utf8`;
DROP TYPE `тип_utf8`;

CREATE TYPE NestedComplex AS Array(Tuple(Map(String, Array(Tuple(String, UInt64))), Nullable(String)));
SHOW TYPE NestedComplex;
DROP TYPE NestedComplex;

CREATE TYPE CaseSensitive AS String;
SHOW TYPE CaseSensitive;
SHOW TYPE casesensitive; -- { serverError UNKNOWN_TYPE }
DROP TYPE CaseSensitive;
