#!/usr/bin/env bash

# A quota limit with an output denominator (`execution_time` is stored in nanoseconds) is scaled through
# a floating-point multiplication. The scaled value must be range-checked before the cast to UInt64:
# an out-of-range value used to be undefined behavior (found by the AST fuzzer with UBSan).
# Quota types without an output denominator (e.g. `queries`) must reject invalid literals too:
# a negative integer used to wrap around to 18446744073709551615 instead of throwing,
# and a positive fractional literal used to be silently truncated (MAX queries = 1.5 became 1).
# The fractional check is done on the literal text: the parsed Float64 has already been rounded
# to the nearest double, so above 2^53 the fraction would vanish before any check on the value
# (9007199254740992.5 rounds to 9007199254740992.0). An exponent moving all fractional digits
# into the integer part (1.5e1 = 15) keeps the literal integral and accepted.
# Quoted (string-literal) limits go through a size-suffix parse that used to skip overflow checking,
# so an out-of-range quoted value silently wrapped around before the range checks; it must throw now.
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
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = -1e-400; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = inf; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = nan; -- { clientError BAD_ARGUMENTS }

CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = -1; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = -1e-400; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = -1.5; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 1.5; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 9007199254740992.5; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 1.5e-400; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 15e-1; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 1e20; -- { clientError CANNOT_CONVERT_TYPE }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = inf; -- { clientError CANNOT_CONVERT_TYPE }

CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = '18446744073709551616'; -- { clientError CANNOT_PARSE_NUMBER }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = '18446744073709551616'; -- { clientError CANNOT_PARSE_NUMBER }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = '18446744073709551T'; -- { clientError CANNOT_PARSE_NUMBER }

-- A reasonable value still works and round-trips.
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = 1.5, MAX queries = 100;
SHOW CREATE QUOTA $QUOTA;

-- A fractional literal whose exponent makes it integral is accepted.
CREATE QUOTA ${QUOTA}_e FOR INTERVAL 1 hour MAX queries = 1.5e1;
SHOW CREATE QUOTA ${QUOTA}_e;

DROP QUOTA $QUOTA;
DROP QUOTA ${QUOTA}_e;
" | sed "s/$QUOTA/q_04617/g"
