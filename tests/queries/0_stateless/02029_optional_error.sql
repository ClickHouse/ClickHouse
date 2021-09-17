DROP TABLE IF EXISTS t;

CREATE TABLE t ( a Int32 ) ENGINE = Memory;

INSERT INTO t VALUES (abs(functionThatDoesNotExists(42))); -- { clientError? UNKNOWN_FUNCTION }
INSERT INTO t VALUES (abs(functionThatDoesNotExists(42))); -- { clientError? }
INSERT INTO t VALUES (abs(functionThatDoesNotExists(42))); -- { clientError }
INSERT INTO t VALUES (1, 2); -- { clientError? SYNTAX_ERROR }
INSERT INTO t VALUES (1, 2); -- { clientError? }
INSERT INTO t VALUES (1, 2); -- { clientError }
INSERT INTO t VALUES (1); -- { clientError? UNKNOWN_FUNCTION }
INSERT INTO t VALUES (1); -- { clientError? SYNTAX_ERROR }
INSERT INTO t VALUES (1); -- { clientError? }

SELECT throwIf(0) FORMAT Null; -- { serverError? NOT_IMPLEMENTED }
SELECT throwIf(0) FORMAT Null; -- { serverError? }
SELECT throwIf(0) FORMAT Null; -- { serverError? FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT throwIf(1) FORMAT Null; -- { serverError? FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT throwIf(1) FORMAT Null; -- { serverError? }
SELECT throwIf(1) FORMAT Null; -- { serverError }

DROP TABLE IF EXISTS t;
