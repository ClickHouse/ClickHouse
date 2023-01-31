#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# We have to use fixed terminal width. It may break other tests results formatting.

backup_stty_size=$(stty size | awk '{print $2}')
stty columns 120
echo "================BINARY=========================="

echo -e "\n"clickhouse --help"\n"
$CLICKHOUSE_BINARY --help

echo -e "\n"clickhouse help"\n"
$CLICKHOUSE_BINARY help
echo -e "\n"clickhouse local"\n"
$CLICKHOUSE_BINARY local --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\n"clickhouse client"\n"
$CLICKHOUSE_BINARY client --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\n"clickhouse benchmark"\n"
$CLICKHOUSE_BINARY benchmark --help | perl -0777 -pe 's/Allowed options:.*\n\n//igs' 
echo -e "\n"clickhouse server"\n"
$CLICKHOUSE_BINARY server --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\n"clickhouse extract"\n"
$CLICKHOUSE_BINARY extract-from-config --help
echo -e "\n"clickhouse compressor"\n"
$CLICKHOUSE_BINARY compressor --help
echo -e "\n"clickhouse format"\n"
$CLICKHOUSE_BINARY format --help
echo -e "\n"clickhouse copier"\n"
$CLICKHOUSE_BINARY copier --help
echo -e "\n"clickhouse obfuscator"\n"
$CLICKHOUSE_BINARY obfuscator --help
echo -e "\n"clickhouse git  "\n"
$CLICKHOUSE_BINARY git-import --help
echo -e "\n"clickhouse keeper"\n"
$CLICKHOUSE_BINARY keeper --help
echo -e "\n"clickhouse keeper"\n"
$CLICKHOUSE_BINARY keeper-converter --help
echo -e "\n"clickhouse install"\n"
$CLICKHOUSE_BINARY install --help
echo -e "\n"clickhouse start"\n"
$CLICKHOUSE_BINARY start --help
echo -e "\n"clickhouse stop"\n"
$CLICKHOUSE_BINARY stop --help
echo -e "\n"clickhouse status"\n"
$CLICKHOUSE_BINARY status --help
echo -e "\n"clickhouse restart"\n"
$CLICKHOUSE_BINARY restart --help
echo -e "\n"clickhouse static"\n"
$CLICKHOUSE_BINARY static-files-disk-uploader --help
echo -e "\n"clickhouse su  "\n"
$CLICKHOUSE_BINARY su --help
echo -e "\n"clickhouse hash"\n"
$CLICKHOUSE_BINARY hash-binary --help | grep -v 'Current binary hash'
echo -e "\n"clickhouse disks"\n"
$CLICKHOUSE_BINARY disks --help | perl -0777 -pe 's/Main options:.*\n\n//igs'

echo "================SYMLINK=============================="

echo -e "\n"clickhouse-local"\n"
${CLICKHOUSE_BINARY}-local --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\n"clickhouse-client"\n"
${CLICKHOUSE_BINARY}-client --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\n"clickhouse-benchmark"\n"
${CLICKHOUSE_BINARY}-benchmark --help | perl -0777 -pe 's/Allowed options:.*\n\n//igs' 
echo -e "\n"clickhouse-server"\n"
${CLICKHOUSE_BINARY}-server --help | perl -0777 -pe 's/Main options:.*\n\n//igs'
echo -e "\n"clickhouse-extract"\n"
${CLICKHOUSE_BINARY}-extract-from-config --help
echo -e "\n"clickhouse-compressor"\n"
${CLICKHOUSE_BINARY}-compressor --help
echo -e "\n"clickhouse-format"\n"
${CLICKHOUSE_BINARY}-format --help
echo -e "\n"clickhouse-copier"\n"
${CLICKHOUSE_BINARY}-copier --help
echo -e "\n"clickhouse-obfuscator"\n"
${CLICKHOUSE_BINARY}-obfuscator --help
echo -e "\n"clickhouse-git  "\n"
${CLICKHOUSE_BINARY}-git-import --help
echo -e "\n"clickhouse-keeper"\n"
${CLICKHOUSE_BINARY}-keeper --help
echo -e "\n"clickhouse-keeper"\n"
${CLICKHOUSE_BINARY}-keeper-converter --help
${CLICKHOUSE_BINARY}-static-files-disk-uploader --help
echo -e "\n"clickhouse-su  "\n"
${CLICKHOUSE_BINARY}-su --help
echo -e "\n"clickhouse-disks"\n"
${CLICKHOUSE_BINARY}-disks --help | perl -0777 -pe 's/Main options:.*\n\n//igs'

stty columns $backup_stty_size
