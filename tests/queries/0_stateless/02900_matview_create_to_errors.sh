#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'create materialized view aaaa TO b EMPTY as Select * from a;' | sed -e 's/Exception/Ex---tion/ ; s/version .*/version reference)/g'
# 'TO ... POPULATE' is allowed now (POPULATE backfills the target table), so it is no longer a syntax error.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'create materialized view aaaa TO b ENGINE = MergeTree() as Select * from a;' | sed -e 's/Exception/Ex---tion/ ; s/version .*/version reference)/g'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d 'create materialized view aaaa ENGINE = MergeTree() TO b as Select * from a;' | sed -e 's/Exception/Ex---tion/ ; s/version .*/version reference)/g'
