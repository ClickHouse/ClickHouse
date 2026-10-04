#!/usr/bin/env bash
# The checks which protect the query cache against storing wrong results (non-deterministic functions, system tables, non-throw overflow
# modes) apply only if the query cache can actually store the result. `clickhouse-local` has a query cache with zero limits, which can
# not store anything, so the queries must run normally there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --query "
    SELECT rand() >= 0 SETTINGS use_query_cache = true;
    SELECT count() FROM system.one SETTINGS use_query_cache = true;
    SELECT count() FROM numbers(10) SETTINGS use_query_cache = true, read_overflow_mode = 'break';"
