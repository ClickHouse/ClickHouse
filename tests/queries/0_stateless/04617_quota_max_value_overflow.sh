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
# into the integer part (1.5e1 = 15) keeps the literal integral and accepted. Digit separators
# are stripped before the analysis, as the number parser strips them (1_5e-1 denotes 1.5).
# The value of such a literal is also computed from the text, because the rounding changes an integer
# above 2^53 too (9007199254740993.0 used to be stored as 9007199254740992).
# Quoted (string-literal) limits go through a size-suffix parse that used to skip overflow checking,
# so an out-of-range quoted value silently wrapped around before the range checks; it must throw now.
# A scaled limit is shown back exactly rather than through a double, so that `SHOW CREATE QUOTA` of a
# value at the top of the range can be replayed instead of being rejected as out of range.
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
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = 18446744073.709551616; -- { clientError BAD_ARGUMENTS }
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
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 1.5_0; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 1_5e-1; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 0x1.8p0; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 1e20; -- { clientError CANNOT_CONVERT_TYPE }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 1.8446744073709551616e19; -- { clientError CANNOT_CONVERT_TYPE }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = 0x1p64; -- { clientError CANNOT_CONVERT_TYPE }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = inf; -- { clientError CANNOT_CONVERT_TYPE }

CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = '18446744073709551616'; -- { clientError CANNOT_PARSE_NUMBER }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = '18446744073709551615'; -- { clientError BAD_ARGUMENTS }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = '18446744073709551616'; -- { clientError CANNOT_PARSE_NUMBER }
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX queries = '18446744073709551T'; -- { clientError CANNOT_PARSE_NUMBER }

-- A reasonable value still works and round-trips.
CREATE QUOTA $QUOTA FOR INTERVAL 1 hour MAX execution_time = 1.5, MAX queries = 100;
SHOW CREATE QUOTA $QUOTA;

-- A fractional literal whose exponent makes it integral is accepted.
CREATE QUOTA ${QUOTA}_e FOR INTERVAL 1 hour MAX queries = 1.5e1;
SHOW CREATE QUOTA ${QUOTA}_e;

-- Digit separators are stripped before the integrality analysis, as the number parser strips them.
CREATE QUOTA ${QUOTA}_u FOR INTERVAL 1 hour MAX queries = 1_5.0e1;
SHOW CREATE QUOTA ${QUOTA}_u;

-- An integer in the floating-point form keeps its exact value above 2^53, where the parsed Float64
-- has already been rounded (9007199254740993.0 would be stored as 9007199254740992).
CREATE QUOTA ${QUOTA}_p FOR INTERVAL 1 hour MAX queries = 9007199254740993.0;
SHOW CREATE QUOTA ${QUOTA}_p;

CREATE QUOTA ${QUOTA}_m FOR INTERVAL 1 hour MAX queries = 18446744073709551615.0;
SHOW CREATE QUOTA ${QUOTA}_m;

-- The same holds for a hexadecimal float, whose exponent counts bits.
CREATE QUOTA ${QUOTA}_h FOR INTERVAL 1 hour MAX queries = 0x20000000000001p0;
SHOW CREATE QUOTA ${QUOTA}_h;

CREATE QUOTA ${QUOTA}_x FOR INTERVAL 1 hour MAX queries = 0x1.8p1;
SHOW CREATE QUOTA ${QUOTA}_x;

-- A hexadecimal mantissa that fits into the range only after a negative exponent shifts its lowest
-- digits away keeps its exact value too (0x200000000000010000000000000000000p-76 is 2^53 + 1).
CREATE QUOTA ${QUOTA}_w FOR INTERVAL 1 hour MAX queries = 0x200000000000010000000000000000000p-76;
SHOW CREATE QUOTA ${QUOTA}_w;

-- An exponent form whose mantissa fits into the range only after the exponent is applied keeps its
-- exact value: the digits that a negative exponent shifts away are discarded before the value is
-- computed (184467440737095516150e-1 is exactly the maximum, while its 21-digit mantissa is not).
CREATE QUOTA ${QUOTA}_n FOR INTERVAL 1 hour MAX queries = 184467440737095516150e-1;
SHOW CREATE QUOTA ${QUOTA}_n;

-- A limit of a quota type with an output denominator is scaled by it exactly: a value at the top of
-- the range must be accepted, while the rounded product of doubles is 2^64 and used to be rejected
-- (18446744073.709551615 nanoseconds is exactly the maximum). The value 18446744073 has no exact
-- double product either. The stored nanoseconds are shown back exactly, not through a double.
CREATE QUOTA ${QUOTA}_s FOR INTERVAL 1 hour MAX execution_time = 18446744073.709551615;
SHOW CREATE QUOTA ${QUOTA}_s;

CREATE QUOTA ${QUOTA}_i FOR INTERVAL 1 hour MAX execution_time = 18446744073;
SHOW CREATE QUOTA ${QUOTA}_i;

-- The same scaled value in the exponent form, whose mantissa does not fit into the range on its own.
CREATE QUOTA ${QUOTA}_o FOR INTERVAL 1 hour MAX execution_time = 184467440737095516150e-10;
SHOW CREATE QUOTA ${QUOTA}_o;

-- A hexadecimal mantissa wider than any fixed accumulator is scaled exactly as well: it fits into
-- the range only after the exponent shifts its lowest bits away, and those bits cannot be dropped
-- beforehand, because the multiplication by the remaining factor 5^n of the denominator carries
-- them upwards (0x100000000000000020000000000000001p-94 seconds is 17179869184.000000001).
CREATE QUOTA ${QUOTA}_v FOR INTERVAL 1 hour MAX execution_time = 0x100000000000000020000000000000001p-94;
SHOW CREATE QUOTA ${QUOTA}_v;

-- A quoted limit of a scaled type is multiplied by the denominator with integer arithmetic too:
-- the double product of '18446744073' seconds and 10^9 loses its low bits, so the stored value
-- used to be 18446744072999999488 nanoseconds and did not round-trip through SHOW CREATE QUOTA.
CREATE QUOTA ${QUOTA}_q FOR INTERVAL 1 hour MAX execution_time = '18446744073';
SHOW CREATE QUOTA ${QUOTA}_q;
" | sed "s/$QUOTA/q_04617/g"

# A scaled limit at the top of the range must be replayable: `SHOW CREATE QUOTA` used to render it
# through a double (18446744073.709553), which is out of range and could not be executed again.
$CLICKHOUSE_CLIENT --query "$($CLICKHOUSE_CLIENT --query "SHOW CREATE QUOTA ${QUOTA}_s" | sed "s/${QUOTA}_s/${QUOTA}_r/")"
$CLICKHOUSE_CLIENT --query "SHOW CREATE QUOTA ${QUOTA}_r" | sed "s/$QUOTA/q_04617/g"

$CLICKHOUSE_CLIENT --query "
DROP QUOTA $QUOTA;
DROP QUOTA ${QUOTA}_e;
DROP QUOTA ${QUOTA}_u;
DROP QUOTA ${QUOTA}_p;
DROP QUOTA ${QUOTA}_m;
DROP QUOTA ${QUOTA}_h;
DROP QUOTA ${QUOTA}_x;
DROP QUOTA ${QUOTA}_w;
DROP QUOTA ${QUOTA}_n;
DROP QUOTA ${QUOTA}_s;
DROP QUOTA ${QUOTA}_i;
DROP QUOTA ${QUOTA}_o;
DROP QUOTA ${QUOTA}_v;
DROP QUOTA ${QUOTA}_q;
DROP QUOTA ${QUOTA}_r;
"
