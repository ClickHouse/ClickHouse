DROP TABLE IF EXISTS t_sql_security_unsupported;

CREATE TABLE t_sql_security_unsupported (x UInt8) ENGINE = Memory SQL SECURITY INVOKER; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_sql_security_unsupported (x UInt8) ENGINE = MergeTree ORDER BY x SQL SECURITY INVOKER; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_sql_security_unsupported SQL SECURITY INVOKER AS numbers(10); -- { clientError SYNTAX_ERROR }

EXISTS TABLE t_sql_security_unsupported;

CREATE TABLE t_sql_security_unsupported (x UInt8) ENGINE = Memory;
DROP TABLE t_sql_security_unsupported;
