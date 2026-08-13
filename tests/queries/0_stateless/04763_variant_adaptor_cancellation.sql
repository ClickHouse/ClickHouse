-- Resolving a function over a Variant argument re-enters the Variant adaptor once per alternative, so
-- with several Variant arguments the work is exponential. The loops must observe query cancellation.

DROP TABLE IF EXISTS t_04763;
DROP TABLE IF EXISTS t_04763_mixed;

CREATE TABLE t_04763 (v Variant(String, UInt64, Array(String), Map(String, String),
                                Tuple(a String), Array(UInt64), Array(Array(String)),
                                Map(UInt64, String)))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04763 VALUES ('a');

CREATE TABLE t_04763_mixed (v Variant(String, UInt64, Array(String), Map(String, String),
                                      Tuple(a String), Array(UInt64), Array(Array(String)),
                                      Map(UInt64, String)))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04763_mixed VALUES ('a'), (42), (['x']);

SELECT 'positive control: one Variant argument resolves and executes normally';
SELECT toTypeName(concat(v, 'z')), concat(v, 'z') FROM t_04763;
SELECT toTypeName(concat(v, 'z')) FROM t_04763_mixed ORDER BY 1 LIMIT 1;

SELECT 'positive control: several Variant arguments still return the right type when not cancelled';
SELECT toTypeName(concat(v, v, v, 'z')), concat(v, v, v, 'z') FROM t_04763;

SELECT 'cancellation is observed while resolving alternatives';
-- Under the 'break' overflow mode an unfixed server runs the whole traversal and returns a result far
-- past the deadline; only a server that polls inside the loop reports the timeout. The 'throw' mode
-- cannot tell the two apart, because the deadline is also reported once the traversal ends.
SELECT toTypeName(concat(v, v, v, v, v, v, v, v, 'z')) FROM t_04763
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }

SELECT 'cancellation is observed with variant_throw_on_type_mismatch disabled';
-- The resolution loop swallows type errors for this setting; a timeout must not be swallowed with them.
SELECT toTypeName(concat(v, v, v, v, v, v, v, v, 'z')) FROM t_04763
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break',
         variant_throw_on_type_mismatch = 0; -- { serverError TIMEOUT_EXCEEDED }

SELECT 'cancellation is observed with several discriminators present in the data';
SELECT toTypeName(concat(v, v, v, v, v, v, v, v, 'z')) FROM t_04763_mixed
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }

DROP TABLE t_04763;
DROP TABLE t_04763_mixed;
