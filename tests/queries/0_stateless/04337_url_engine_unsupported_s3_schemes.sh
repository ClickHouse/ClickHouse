#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: exercises the URL engine scheme dispatch.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The unified `URL` engine / `url` table function only dispatch to the `S3` engine the schemes that
# the S3 URI mapper resolves to a concrete endpoint without any configuration: the native `s3`, plus
# `gs`/`gcs`/`oss` (default `url_scheme_mappers` + the built-in fallback). Other S3-compatible vendor
# schemes (`cos`, `cosn`, `obs`, `eos`, `s3express`, ...) are region-specific virtual-hosted
# hostnames with no default endpoint mapping. They must NOT be silently routed to `S3::URI` (which
# would parse `<scheme>://bucket/key` as a custom endpoint and take the object key as the bucket);
# instead they stay on the plain `URL` path and fail with a clear scheme error.

# `url(...)` table function: each unsupported scheme stays on the plain URL path and is rejected with
# a scheme/protocol error, not misparsed as an S3 bucket/key.
for scheme in cos cosn obs eos s3express; do
    echo "--- url('${scheme}://...') is not dispatched to S3 (rejected as a plain-URL scheme) ---"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM url('${scheme}://bucket/key', 'CSV', 'a UInt32')" 2>&1 \
        | grep -qiE "Unknown protocol in the URL|Unsupported scheme in URI" && echo "rejected" || echo "NOT REJECTED"
done

# `ENGINE = URL('obs://...')`: the table is created as a plain `URL` engine (not S3-backed), and a
# read fails with the same scheme error. Creation with explicit columns performs no I/O.
echo "--- ENGINE = URL('obs://...') is created as a plain URL engine, not S3 ---"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_obs"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_obs (a UInt32) ENGINE = URL('obs://bucket/key', 'CSV')"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_obs'"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_obs" 2>&1 \
    | grep -qiE "Unknown protocol in the URL|Unsupported scheme in URI" && echo "rejected" || echo "NOT REJECTED"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_obs"
