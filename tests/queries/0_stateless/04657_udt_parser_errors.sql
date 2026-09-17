-- parser and feature gate parser failures are exercised through formatQuery so the expected
-- error is produced by the server parser, independently of client multiquery
-- statement splitting.

SELECT 'create-and-attach' FORMAT TSVRaw;

SELECT formatQuery('CREATE OR REPLACE TYPE udt_04657.T AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T.More AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T() AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(X) AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(X Mystery) AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(X TYPE, X UInt8) AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(X UInt8 = 1) AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(X TYPE...) AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(X TYPE,) AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T UUID ''01234567-89ab-cdef-0123-456789abcdef'' AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T REVISION 1 AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T ON CLUSTER c ON CLUSTER d AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 COMMENT ''one'' COMMENT ''two'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T AS'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 AS UInt8'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 COMMENT ''bad\0comment'''); -- { serverError SYNTAX_ERROR }

SELECT formatQuery('ATTACH TYPE udt_04657.T AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T REVISION 1 AS UInt64 DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''01234567-89ab-cdef-0123-456789abcdef'' AS UInt64 DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''01234567-89ab-cdef-0123-456789abcdef'' REVISION 1 AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''00000000-0000-0000-0000-000000000000'' REVISION 1 AS UInt64 DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''not-a-uuid'' REVISION 1 AS UInt64 DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'''); -- { serverError CANNOT_PARSE_UUID }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''01234567-89ab-cdef-0123-456789abcdef'' REVISION 0 AS UInt64 DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''01234567-89ab-cdef-0123-456789abcdef'' REVISION 1 AS UInt64 DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''01234567-89ab-cdef-0123-456789abcdef'' REVISION 1 AS UInt64 DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''01234567-89ab-cdef-0123-456789abcdef'' REVISION 1 AS UInt64 DEFINITION HASH ''g123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''01234567-89ab-cdef-0123-456789abcdef'' UUID ''11111111-1111-1111-1111-111111111111'' REVISION 1 AS UInt64 DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''01234567-89ab-cdef-0123-456789abcdef'' REVISION 1 REVISION 2 AS UInt64 DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''01234567-89ab-cdef-0123-456789abcdef'' REVISION 1 AS UInt64 DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'' DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ATTACH TYPE udt_04657.T UUID ''01234567-89ab-cdef-0123-456789abcdef'' REVISION 1 AS UInt64 COMMENT ''out of order'' DEFINITION HASH ''0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'''); -- { serverError SYNTAX_ERROR }

SELECT 'decreases' FORMAT TSVRaw;

SELECT formatQuery('CREATE TYPE udt_04657.T(T TYPE) DECREASES T AS UInt8'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(B Bool) DECREASES B AS UInt8'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N Int8) DECREASES N AS UInt8'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N Int16) DECREASES N AS UInt8'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N Int32) DECREASES N AS UInt8'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N Int64) DECREASES N AS UInt8'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(S String) DECREASES S AS UInt8'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N UInt16) DECREASES Missing AS UInt8'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N UInt16) DECREASES N DECREASES N AS UInt8'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N UInt16) DECREASES AS UInt8'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N UInt16) AS UInt8 DECREASES N'); -- { serverError SYNTAX_ERROR }

SELECT 'templates' FORMAT TSVRaw;

SELECT formatQuery('CREATE TYPE udt_04657.T(N UInt16) AS N'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(T TYPE) AS T(UInt8)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(B Bool) AS TYPE_IF(B = 0, UInt8, UInt16)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(S String) AS TYPE_IF(S = 0, UInt8, UInt16)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(T TYPE) AS TYPE_IF(T = 0, UInt8, UInt16)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N UInt16) AS TYPE_IF(Missing = 0, UInt8, UInt16)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N UInt16) AS TYPE_IF(N = 1, UInt8, UInt16)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N UInt16) AS TYPE_IF(N > 0, UInt8, UInt16)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N UInt16) AS TYPE_IF(N = 0, UInt8)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(N UInt16) AS TYPE_IF(N = 0, UInt8, UInt16, UInt32)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(T TYPE, N UInt16) DECREASES N AS udt_04657.T(T, N)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(T TYPE, N UInt16) DECREASES N AS udt_04657.T(T, N - 2)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(T TYPE, N UInt16, M UInt16) DECREASES N AS udt_04657.T(T, N, M - 1)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(T TYPE, N UInt16) DECREASES N AS udt_04657.T(T)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(T TYPE, N UInt16) DECREASES N AS udt_04657.T(T, N - 1, UInt8)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(T TYPE, N UInt16) DECREASES N AS udt_04657.T(UInt8, N - 1)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T(T TYPE, N UInt16) DECREASES N AS udt_04657.T(T, N - 1,)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('CREATE TYPE udt_04657.T AS udt_04657.Other(UInt8,)'); -- { serverError SYNTAX_ERROR }

SELECT 'qualification' FORMAT TSVRaw;

SELECT formatQuery('CREATE TYPE udt_04657.T AS other.db.Type'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('DROP TYPE udt_04657.T.More'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TYPE udt_04657.T.More RENAME TO U'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SHOW CREATE TYPE udt_04657.T.More'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('DESCRIBE TYPE udt_04657.T.More'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES OBJECT TABLE udt_04657.t.more DRY RUN'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SELECT CAST(1 AS udt_04657.T.More)'); -- { serverError SYNTAX_ERROR }

SELECT 'lifecycle-and-introspection' FORMAT TSVRaw;

SELECT formatQuery('ALTER TYPE udt_04657.T AS UInt64'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TYPE udt_04657.T COMMENT ''x''');
SELECT formatQuery('ALTER TYPE udt_04657.T RENAME TO other.U'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TYPE udt_04657.T ON CLUSTER c ON CLUSTER d RENAME TO U'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TYPE udt_04657.T RENAME TO U RENAME TO V'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('DROP TYPE udt_04657.T CASCADE'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('DROP TYPE udt_04657.T RESTRICT RESTRICT'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('DROP TYPE udt_04657.T ON CLUSTER c ON CLUSTER d'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SHOW TYPES LIKE ''T%'' FROM udt_04657'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SHOW TYPES FROM udt_04657 FROM other'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SHOW TYPES FROM udt_04657 LIKE ''T%'' LIKE ''U%'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SHOW TYPES FROM'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SHOW TYPES LIKE'); -- { serverError SYNTAX_ERROR }

SELECT 'physicalize' FORMAT TSVRaw;

SELECT formatQuery('PHYSICALIZE TYPE REFERENCES'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES OBJECT'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES OBJECT TABLE'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES CLOSURE TABLE udt_04657.t DRY RUN'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES CLOSURE OF TABLE DRY RUN'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES DATABASE'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES OBJECT TABLE udt_04657.t'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES OBJECT TABLE udt_04657.t OBJECT VIEW udt_04657.v DRY RUN'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES OBJECT TABLE udt_04657.t ON CLUSTER c ON CLUSTER d DRY RUN'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES OBJECT TABLE udt_04657.t DROP UNUSED TYPES DROP UNUSED TYPES DRY RUN'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES OBJECT TABLE udt_04657.t DRY RUN DRY RUN'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES OBJECT TABLE udt_04657.t APPLY DRY RUN'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES APPLY TOKEN'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES APPLY TOKEN token'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES APPLY TOKEN ''x'' ON CLUSTER c'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES APPLY TOKEN ''x'' DROP UNUSED TYPES'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES APPLY TOKEN ''x'' DRY RUN'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES APPLY TOKEN ''a\0b'''); -- { serverError SYNTAX_ERROR }
SELECT formatQuery(format('PHYSICALIZE TYPE REFERENCES APPLY TOKEN ''{}''', repeat('x', 4097))); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('PHYSICALIZE TYPE REFERENCES APPLY TOKEN ''unterminated'); -- { serverError SYNTAX_ERROR }

SELECT 'inactive-before-body' FORMAT TSVRaw;

SELECT formatQuery('CREATE TYPE udt_04657.T INPUT deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T OUTPUT deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T DEFAULT deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T CONSTRAINT deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T CHECK deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T PRIMARY KEY deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T FOREIGN KEY deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T UNIQUE deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }

SELECT 'inactive-after-body' FORMAT TSVRaw;

SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 INPUT deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 OUTPUT deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 DEFAULT deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 CONSTRAINT deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 CHECK deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 PRIMARY KEY deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 FOREIGN KEY deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }
SELECT formatQuery('CREATE TYPE udt_04657.T AS UInt64 UNIQUE deliberately malformed'); -- { serverError UNSUPPORTED_TYPE_CLAUSE }

SELECT 'ordinary-fallback' FORMAT TSVRaw;

SELECT startsWith(formatQuery('SHOW CREATE TYPE'), 'SHOW CREATE TABLE');
SELECT startsWith(formatQuery('SHOW CREATE TYPE FORMAT JSON'), 'SHOW CREATE TABLE');
SELECT startsWith(formatQuery('DESCRIBE TYPE'), 'DESCRIBE TABLE');
SELECT startsWith(formatQuery('DESCRIBE TYPE alias'), 'DESCRIBE TABLE');
SELECT startsWith(formatQuery('DESCRIBE TYPE FORMAT JSON'), 'DESCRIBE TABLE');

SELECT 'done' FORMAT TSVRaw;
