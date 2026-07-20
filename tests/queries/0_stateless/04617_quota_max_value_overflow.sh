#!/usr/bin/env bash

# A quota limit with an output denominator (`execution_time` is stored in nanoseconds) is scaled through
# a floating-point multiplication. The scaled value must be range-checked before the cast to UInt64:
# an out-of-range value used to be undefined behavior (found by the AST fuzzer with UBSan).
# Quota types without an output denominator (e.g. `queries`) must reject invalid literals too:
# a negative integer used to wrap around to 18446744073709551615 instead of throwing.
# Note: a `--` comment attached to a hinted query would shadow its test hint, so the queries below carry no leading SQL comments.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The quota name must be unique per test run: quotas are global entities,
# and concurrent runs of this test (e.g. the flaky check) would collide on a fixed name.
QUOTA="q_04617_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP QUOTA IF EXISTS $QUOTA;

CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = 1e19; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = 18446744073709551615; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = -1; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = inf; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = nan; -- { clientError BAD_ARGUMENTS }

CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = -1; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = -1.5; -- { clientError CANNOT_CONVERT_TYPE }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 1e20; -- { clientError CANNOT_CONVERT_TYPE }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = inf; -- { clientError CANNOT_CONVERT_TYPE }

-- A reasonable value still works and round-trips.
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = 1.5, MAX queries = 100;
SHOW CREATE QUOTA $QUOTA;

DROP QUOTA $QUOTA;
" | sed "s/$QUOTA/q_04617/g"
