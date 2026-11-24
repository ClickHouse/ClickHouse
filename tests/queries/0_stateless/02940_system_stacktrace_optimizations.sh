#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# NOTE: due to grep "Cannot obtain a stack trace for thread {}' will be ignored automatically, which is the intention.

# no message at all
echo "thread = 0"
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=test -m -q "select * from system.stack_trace where thread_id = 0" |& grep -F -o 'Send signal to'

# send messages to some threads
echo "thread != 0"
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=test -m -q "select * from system.stack_trace where thread_id != 0 format Null" |& grep -F -o 'Send signal to' | grep -v 'Send signal to 0 threads (total)'

# there is no thread with comm="foo", so no signals will be sent
echo "thread_name = 'foo'"
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=test -m -q "select * from system.stack_trace where thread_name = 'foo' format Null" |& grep -F -o 'Send signal to 0 threads (total)'
