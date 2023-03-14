#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# We have to use fixed terminal width. It may break other tests results formatting.
# In CI there is no tty and we just ignore failed stty calls.
# Set 80 to have same as default size as in notty.
backup_stty_size=$(stty size 2>/dev/null | awk '{print $2}' ||:)
stty columns 78 2>/dev/null ||:

echo "================BINARY=========================="

echo -e "\nclickhouse --help\n"
$CLICKHOUSE_BINARY --help
echo -e "\nclickhouse help\n"
$CLICKHOUSE_BINARY help

echo -e "\nclickhouse server\n"
$CLICKHOUSE_BINARY server --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\nclickhouse copier\n"
$CLICKHOUSE_BINARY copier --help
echo -e "\nclickhouse keeper\n"
$CLICKHOUSE_BINARY keeper --help

echo "================SYMLINK=============================="

echo -e "\nclickhouse-server\n"
${CLICKHOUSE_BINARY}-server --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\nclickhouse-copier\n"
${CLICKHOUSE_BINARY}-copier --help
echo -e "\nclickhouse-keeper\n"
${CLICKHOUSE_BINARY}-keeper --help

stty columns $backup_stty_size 2>/dev/null ||:
