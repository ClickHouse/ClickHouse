DROP TYPE IF EXISTS UserId;
DROP TYPE IF EXISTS Email;
DROP TYPE IF EXISTS Phone;
DROP TYPE IF EXISTS ComplexArray;
DROP TYPE IF EXISTS ParameterizedType;

CREATE TYPE UserId AS UInt64 
    INPUT 'toUInt64(assumeNotNull(value))' 
    OUTPUT 'toString(value)' 
    DEFAULT '0';
SHOW TYPE UserId;

CREATE TYPE ComplexArray AS Array(Tuple(String, UInt64));
SHOW TYPE ComplexArray;

CREATE TYPE ParameterizedType(T, U) AS Tuple(T, Array(U));
SHOW TYPE ParameterizedType;

CREATE TYPE UserIdArray AS Array(UserId);
SHOW TYPE UserIdArray;

SHOW TYPES;

DROP TYPE UserId;
DROP TYPE ComplexArray;
DROP TYPE ParameterizedType;
DROP TYPE UserIdArray;

SHOW TYPES;
