-- Regression test: with only some arguments `Nullable`, the result of `arrayIntersect` must follow
-- the declared return type. Deriving it from the arguments instead returned a `Nullable` column for
-- a not `Nullable` return type (`Date32`), and put a `NULL` into an intersection that cannot hold
-- one (`UUID`).

SELECT arrayIntersect(CAST([toDate32('2000-01-01')] AS Array(Nullable(Date32))), [toDate32('2000-01-01')]);
SELECT toTypeName(arrayIntersect(CAST([] AS Array(Nullable(Date32))), CAST([] AS Array(Date32))));
SELECT arrayIntersect(CAST([toDate32('2000-01-01')] AS Array(Date32)), CAST([NULL] AS Array(Nullable(Date32))));
SELECT arrayIntersect(CAST([NULL] AS Array(Nullable(UUID))), [toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')]);
SELECT arrayIntersect(CAST([NULL] AS Array(Nullable(UUID))), CAST([NULL] AS Array(Nullable(UUID))));
SELECT arrayUnion(CAST([NULL] AS Array(Nullable(UUID))), [toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')]);
