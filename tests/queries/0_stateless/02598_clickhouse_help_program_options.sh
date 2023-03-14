#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# We have to use fixed terminal width. It may break other tests results formatting.
# In CI there is no tty and we just ignore failed stty calls.
# Set 80 to have same as default size as in notty.
backup_stty_size=$(stty size 2>/dev/null | awk '{print $2}' ||:)
stty columns 80 2>/dev/null ||:

echo "================BINARY=========================="

echo -e "\nclickhouse --help\n"
$CLICKHOUSE_BINARY --help
echo -e "\nclickhouse help\n"
$CLICKHOUSE_BINARY help

echo -e "\nclickhouse benchmark\n"
$CLICKHOUSE_BINARY benchmark --help | perl -0777 -pe 's/Allowed options:.*\n\n//igs' 
echo -e "\nclickhouse client\n"
$CLICKHOUSE_BINARY client --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\nclickhouse local\n"
$CLICKHOUSE_BINARY local --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\nclickhouse compressor\n"
$CLICKHOUSE_BINARY compressor --help
echo -e "\nclickhouse disks\n"
$CLICKHOUSE_BINARY disks --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\nclickhouse extract\n"
$CLICKHOUSE_BINARY extract-from-config --help
echo -e "\nclickhouse format\n"
$CLICKHOUSE_BINARY format --help
echo -e "\nclickhouse git-import\n"
$CLICKHOUSE_BINARY git-import --help
echo -e "\nclickhouse install\n"
$CLICKHOUSE_BINARY install --help
echo -e "\nclickhouse keeper-converter\n"
$CLICKHOUSE_BINARY keeper-converter --help
echo -e "\nclickhouse obfuscator\n"
$CLICKHOUSE_BINARY obfuscator --help
echo -e "\nclickhouse static\n"
$CLICKHOUSE_BINARY static-files-disk-uploader --help



echo -e "\nclickhouse start\n"
$CLICKHOUSE_BINARY start --help
echo -e "\nclickhouse stop\n"
$CLICKHOUSE_BINARY stop --help
echo -e "\nclickhouse status\n"
$CLICKHOUSE_BINARY status --help
echo -e "\nclickhouse restart\n"
$CLICKHOUSE_BINARY restart --help
echo -e "\nclickhouse su\n"
$CLICKHOUSE_BINARY su --help
echo -e "\nclickhouse hash\n"
$CLICKHOUSE_BINARY hash-binary --help | grep -v 'Current binary hash'

echo "================SYMLINK=============================="

echo -e "\nclickhouse-local\n"
${CLICKHOUSE_BINARY}-local --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\nclickhouse-client\n"
${CLICKHOUSE_BINARY}-client --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\nclickhouse-benchmark\n"
${CLICKHOUSE_BINARY}-benchmark --help | perl -0777 -pe 's/Allowed options:.*\n\n//igs' 
echo -e "\nclickhouse-extract\n"
${CLICKHOUSE_BINARY}-extract-from-config --help
echo -e "\nclickhouse-compressor\n"
${CLICKHOUSE_BINARY}-compressor --help
echo -e "\nclickhouse-format\n"
${CLICKHOUSE_BINARY}-format --help
echo -e "\nclickhouse-obfuscator\n"
${CLICKHOUSE_BINARY}-obfuscator --help
echo -e "\nclickhouse-git-import\n"
${CLICKHOUSE_BINARY}-git-import --help
echo -e "\nclickhouse-keeper-converter\n"
${CLICKHOUSE_BINARY}-keeper-converter --help
echo -e "\nclickhouse-static-files-disk-uploader\n"
${CLICKHOUSE_BINARY}-static-files-disk-uploader --help
echo -e "\nclickhouse-su\n"
${CLICKHOUSE_BINARY}-su --help
echo -e "\nclickhouse-disks\n"
${CLICKHOUSE_BINARY}-disks --help | perl -0777 -pe 's/Main options:.*\n\n//igs'

stty columns $backup_stty_size 2>/dev/null ||:
