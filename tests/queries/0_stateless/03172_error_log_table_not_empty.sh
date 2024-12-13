#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: this test relies on the timeouts, it always takes no less that 4 seconds to run

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# system.error_log is created lazy, flush logs query makes it sure that the table is created.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS;"

# Get the previous number of errors for 111, 222 and 333
errors_111=$($CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM system.error_log WHERE code = 111")
errors_222=$($CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM system.error_log WHERE code = 222")
errors_333=$($CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM system.error_log WHERE code = 333")

# Throw three random errors: 111, 222 and 333 and wait for more than collect_interval_milliseconds to ensure system.error_log is flushed
$CLICKHOUSE_CLIENT -m -q "
SELECT throwIf(true, 'error_log', toInt16(111)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 111 }
SELECT throwIf(true, 'error_log', toInt16(222)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 222 }
SELECT throwIf(true, 'error_log', toInt16(333)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 333 }
SELECT sleep(2) format NULL;
SYSTEM FLUSH LOGS;
"

# Check that the three random errors are propagated
$CLICKHOUSE_CLIENT -m -q "
SELECT sum(value) > $errors_111 FROM system.error_log WHERE code = 111;
SELECT sum(value) > $errors_222 FROM system.error_log WHERE code = 222;
SELECT sum(value) > $errors_333 FROM system.error_log WHERE code = 333;
"

# Ensure that if we throw them again, they're still propagated
$CLICKHOUSE_CLIENT -m -q "
SELECT throwIf(true, 'error_log', toInt16(111)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 111 }
SELECT throwIf(true, 'error_log', toInt16(222)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 222 }
SELECT throwIf(true, 'error_log', toInt16(333)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 333 }
SELECT sleep(2) format NULL;
SYSTEM FLUSH LOGS;
"

$CLICKHOUSE_CLIENT -m -q "
SELECT sum(value) > $(($errors_111+1)) FROM system.error_log WHERE code = 111;
SELECT sum(value) > $(($errors_222+1)) FROM system.error_log WHERE code = 222;
SELECT sum(value) > $(($errors_333+1)) FROM system.error_log WHERE code = 333;
"
