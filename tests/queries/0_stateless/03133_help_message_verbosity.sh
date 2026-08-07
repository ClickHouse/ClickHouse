#!/usr/bin/env bash

# Checks that "clickhouse-client/local --help" prints a brief summary of CLI arguments and "--help --verbose" prints all possible CLI arguments
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unique identifier for concurrent execution
PID=$$

# Get the help message in short and verbose form and put them into txt files
$CLICKHOUSE_CLIENT --help > "help_msg_$PID.txt"
$CLICKHOUSE_CLIENT --help --verbose > "verbose_help_msg_$PID.txt"

# Sizes of files
size_short=$(stat -c %s "help_msg_$PID.txt")
size_verbose=$(stat -c %s "verbose_help_msg_$PID.txt")

# If the size of the short help message is less, everything is OK
if [ $size_short -lt $size_verbose ]; then
  echo "OK"
else
  echo "Not OK"
fi

rm "help_msg_$PID.txt"
rm "verbose_help_msg_$PID.txt"

# The same for clickhouse local
$CLICKHOUSE_LOCAL --help > "help_msg_$PID.txt"
$CLICKHOUSE_LOCAL --help --verbose > "verbose_help_msg_$PID.txt"

size_short=$(stat -c %s "help_msg_$PID.txt")
size_verbose=$(stat -c %s "verbose_help_msg_$PID.txt")

if [ $size_short -lt $size_verbose ]; then
  echo "OK"
else
  echo "Not OK"
fi

rm "help_msg_$PID.txt"
rm "verbose_help_msg_$PID.txt"
