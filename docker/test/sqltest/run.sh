#!/bin/bash
# shellcheck disable=SC2015

set -x
set -e
set -u
set -o pipefail

BINARY_TO_DOWNLOAD=${BINARY_TO_DOWNLOAD:="clang-18_debug_none_unsplitted_disable_False_binary"}
BINARY_URL_TO_DOWNLOAD=${BINARY_URL_TO_DOWNLOAD:="https://clickhouse-builds.s3.amazonaws.com/$PR_TO_TEST/$SHA_TO_TEST/clickhouse_build_check/$BINARY_TO_DOWNLOAD/clickhouse"}

function wget_with_retry
{
    for _ in 1 2 3 4; do
        if wget -nv -nd -c "$1";then
            return 0
        else
            sleep 0.5
        fi
    done
    return 1
}

wget_with_retry "$BINARY_URL_TO_DOWNLOAD"
chmod +x clickhouse
./clickhouse install --noninteractive

echo "
users:
  default:
    access_management: 1" > /etc/clickhouse-server/users.d/access_management.yaml

clickhouse start

# Wait for start
for _ in {1..100}
do
    clickhouse-client --query "SELECT 1" && break ||:
    sleep 1
done

# Run the test
pushd sqltest/standards/2016/
/test.py
mv report.html test.log /workspace
popd

zstd --threads=0 /var/log/clickhouse-server/clickhouse-server.log
zstd --threads=0 /var/log/clickhouse-server/clickhouse-server.err.log

mv /var/log/clickhouse-server/clickhouse-server.log.zst /var/log/clickhouse-server/clickhouse-server.err.log.zst /workspace
