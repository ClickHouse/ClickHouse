-- A quota limit with an output denominator (`execution_time` is stored in nanoseconds) is scaled through
-- a floating-point multiplication. The scaled value must be range-checked before the cast to UInt64:
-- an out-of-range value used to be undefined behavior (found by the AST fuzzer with UBSan).

DROP QUOTA IF EXISTS q_04617;

CREATE QUOTA q_04617 FOR INTERVAL 1 hour MAX execution_time = 1e19; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA q_04617 FOR INTERVAL 1 hour MAX execution_time = 18446744073709551615; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA q_04617 FOR INTERVAL 1 hour MAX execution_time = -1; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA q_04617 FOR INTERVAL 1 hour MAX execution_time = inf; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA q_04617 FOR INTERVAL 1 hour MAX execution_time = nan; -- { clientError BAD_ARGUMENTS }

-- A reasonable value still works and round-trips.
CREATE QUOTA q_04617 FOR INTERVAL 1 hour MAX execution_time = 1.5;
SHOW CREATE QUOTA q_04617;

DROP QUOTA q_04617;
