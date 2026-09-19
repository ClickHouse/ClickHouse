-- Tags: log-engine
-- A wrong number of engine arguments is rejected for `File` and for the `Log` family.

DROP TABLE IF EXISTS t_engine_args;

CREATE TABLE t_engine_args (a UInt64) ENGINE = File(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE t_engine_args (a UInt64) ENGINE = File(CSV, 'a.csv', 'none', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE t_engine_args (a UInt64) ENGINE = Log('x'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE t_engine_args (a UInt64) ENGINE = TinyLog('x'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE t_engine_args (a UInt64) ENGINE = StripeLog('x'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_engine_args';

DROP TABLE IF EXISTS t_engine_args;
