#!/usr/bin/env bash
# Tags: long, no-fasttest, no-tsan
# FIXME It became flaky after upgrading to llvm-14 due to obscure freezes in tsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: zstd'              "${CLICKHOUSE_URL}&enable_http_compression=1" -d "SELECT toDate('2020-12-12') as datetime, 'test-pipeline' as pipeline, 'clickhouse-test-host-001.clickhouse.com' as host, 'clickhouse' as home, 'clickhouse' as detail, number as row_number FROM numbers(1000000) SETTINGS max_block_size=65505 FORMAT JSON" | zstd -d | tail -n30 | head -n23
