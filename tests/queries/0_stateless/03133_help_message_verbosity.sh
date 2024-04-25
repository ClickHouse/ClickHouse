#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Get the help message in short and vebose form and put them into txt files
$CLICKHOUSE_CLIENT --help > help_msg.txt
$CLICKHOUSE_CLIENT --help --verbose > verbose_help_msg.txt

# Sizes of files
size_short=$(stat -c %s help_msg.txt)
size_verbose=$(stat -c %s verbose_help_msg.txt)

# If the size of the short help message is less, everything is OK
if [ $size_short -lt $size_verbose ]; then
  echo "OK"
else
  echo "Not OK"
fi

rm help_msg.txt
rm verbose_help_msg.txt


# The same for clickhouse local
$CLICKHOUSE_LOCAL --help > help_msg.txt
$CLICKHOUSE_LOCAL --help --verbose > verbose_help_msg.txt

size_short=$(stat -c %s help_msg.txt)
size_verbose=$(stat -c %s verbose_help_msg.txt)

if [ $size_short -lt $size_verbose ]; then
  echo "OK"
else
  echo "Not OK"
fi

rm help_msg.txt
rm verbose_help_msg.txt
