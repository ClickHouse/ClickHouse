#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses s3Cluster/urlCluster/fileCluster over Minio and the HTTP port

# A SQL SECURITY DEFINER/NONE view runs its inner query in a context rebuilt from the global
# context, which used to drop the cluster-function read-task callback. When such a view was read as
# a secondary query the *Cluster function inside it could no longer reach the initiator that decides
# whether to serve the read task, and hit a LOGICAL_ERROR that aborts the server in debug builds.
# The callback is now propagated, so the unsupported nesting is rejected with the same BAD_ARGUMENTS
# an INVOKER view already produced, and the server stays alive.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

DATA_FILE="${CLICKHOUSE_DATABASE}_04810.tsv"
printf '1\n2\n3\n' > "${USER_FILES_PATH}/${DATA_FILE}"

S3_OBJECT="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_04810.tsv"
$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3('${S3_OBJECT}', 'TSV', 'x UInt8') SELECT 1 SETTINGS s3_truncate_on_insert = 1"

S3_FN="s3Cluster('test_cluster_two_shards_localhost', '${S3_OBJECT}', 'TSV', 'x UInt8')"
URL_FN="urlCluster('test_cluster_two_shards_localhost', 'http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8')"
FILE_FN="fileCluster('test_cluster_two_shards_localhost', '${DATA_FILE}', 'TSV', 'x UInt8')"

# prefer_localhost_replica=0 makes the outer remote() ship the view read to the local shard as a
# real secondary query, which is what turns on distributed processing inside the view. Without it
# the read stays an initial query and the callback is never needed.
# INVOKER is the control: it never lost the callback, so its verdict must be identical throughout.
for security in DEFINER NONE INVOKER; do
    for fn in s3Cluster urlCluster fileCluster; do
        case "$fn" in
            s3Cluster) inner="$S3_FN" ;;
            urlCluster) inner="$URL_FN" ;;
            fileCluster) inner="$FILE_FN" ;;
        esac

        $CLICKHOUSE_CLIENT --query "CREATE OR REPLACE VIEW v_04810 SQL SECURITY ${security} AS SELECT * FROM ${inner}"

        for analyzer in 1 0; do
            echo "--- ${security} / ${fn} / enable_analyzer=${analyzer} ---"
            $CLICKHOUSE_CLIENT --query "
                SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 'v_04810')
                SETTINGS prefer_localhost_replica = 0, enable_analyzer = ${analyzer}
            " 2>&1 | grep -o -m1 "cannot be nested inside another distributed query"
        done
    done
done

$CLICKHOUSE_CLIENT --query "DROP VIEW IF EXISTS v_04810"

# A definer view over a cluster function read locally is a supported query and must keep working.
echo "--- local read of a DEFINER view still works ---"
$CLICKHOUSE_CLIENT --query "CREATE OR REPLACE VIEW v_04810 SQL SECURITY DEFINER AS SELECT * FROM ${S3_FN}"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM v_04810"
$CLICKHOUSE_CLIENT --query "DROP VIEW v_04810"

# The regression being prevented: BAD_ARGUMENTS must not abort the server.
echo "--- server alive ---"
$CLICKHOUSE_CLIENT --query "SELECT 1"

rm -f "${USER_FILES_PATH}/${DATA_FILE}"
