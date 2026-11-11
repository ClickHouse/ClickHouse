-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.
DROP TYPE IF EXISTS UserIdArray;
DROP TYPE IF EXISTS UserId;
DROP TYPE IF EXISTS ComplexArray;
DROP TYPE IF EXISTS ParameterizedType;

CREATE TYPE UserId AS UInt64;
SHOW TYPE UserId;

CREATE TYPE ComplexArray AS Array(Tuple(String, UInt64));
SHOW TYPE ComplexArray;

CREATE TYPE ParameterizedType(T, U) AS Tuple(T, Array(U));
SHOW TYPE ParameterizedType;

CREATE TYPE UserIdArray AS Array(UserId);
SHOW TYPE UserIdArray;

SHOW TYPES;

-- A type another type is defined through can not be dropped first.
DROP TYPE UserId; -- { serverError BAD_ARGUMENTS }
DROP TYPE UserIdArray;
DROP TYPE UserId;
DROP TYPE ComplexArray;
DROP TYPE ParameterizedType;

SHOW TYPES;
