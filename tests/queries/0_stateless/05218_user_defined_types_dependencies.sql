-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.

DROP TYPE IF EXISTS DepUser;
DROP TYPE IF EXISTS DepList;
DROP TYPE IF EXISTS DepBase;
DROP TYPE IF EXISTS DepBroken;

CREATE TYPE DepBase AS UInt64;
CREATE TYPE DepList(T) AS Array(T);
CREATE TYPE DepUser AS Tuple(DepBase, DepList(String));
SELECT toTypeName(CAST((1, ['a']), 'DepUser'));

-- A type that other types are defined through can not be dropped.
DROP TYPE DepBase; -- { serverError BAD_ARGUMENTS }
DROP TYPE DepList; -- { serverError BAD_ARGUMENTS }
SHOW TYPES;

-- A reference to a user-defined type with a wrong number of arguments is rejected at definition time.
CREATE TYPE DepBroken AS DepList(String, UInt8); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TYPE DepBroken AS DepList; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TYPE DepBroken AS DepBase(UInt8); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TYPE DepBroken(T) AS Array(DepList(T, T)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SHOW TYPE DepBroken; -- { serverError UNKNOWN_TYPE }

-- A replacement may not make a type depend on itself, directly or through other types...
CREATE TYPE OR REPLACE DepBase AS Array(DepBase); -- { serverError BAD_ARGUMENTS }
CREATE TYPE OR REPLACE DepBase AS DepUser; -- { serverError BAD_ARGUMENTS }
CREATE TYPE OR REPLACE DepBase AS Map(String, DepList(DepUser)); -- { serverError BAD_ARGUMENTS }
-- ...nor change the number of parameters other types use it with.
CREATE TYPE OR REPLACE DepBase(T) AS Array(T); -- { serverError BAD_ARGUMENTS }
CREATE TYPE OR REPLACE DepList AS Array(String); -- { serverError BAD_ARGUMENTS }
SHOW TYPE DepBase;
SELECT toTypeName(CAST((1, ['a']), 'DepUser'));

-- A compatible replacement is seen through the dependent type.
CREATE TYPE OR REPLACE DepBase AS String;
CREATE TYPE OR REPLACE DepList(T) AS Array(Nullable(T));
SHOW TYPE DepBase;
SELECT toTypeName(CAST(('x', ['a']), 'DepUser'));

-- A parameter shadows a user-defined type of the same name, so it is not a dependency.
CREATE TYPE DepBroken(DepBase) AS Array(DepBase);
SELECT toTypeName(CAST([1], 'DepBroken(UInt8)'));
DROP TYPE DepBroken;

DROP TYPE DepUser;
DROP TYPE DepList;
DROP TYPE DepBase;
SHOW TYPES;
