#!/usr/bin/env bash
# Tags: long
# Tag long: schema inference walks a batch of 1000 empty addresses before the one it is after.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The lazy glob expansion feeds schema inference in batches of 1000 addresses, and every batch must
# get the schema-cache pass the first one gets. Address 0 through 999 answer with an empty body and
# are skipped; the schema of address 1200 is put into the cache up front, so inference over the
# pattern has to find it there once the second batch appears, instead of reading on. The cache hit
# is what distinguishes the outcomes - the inferred schema would come out the same - so it is what
# the test looks for.

# The address answers with one row of `0` when its number is at least 1000 and with nothing below
# that. `%3E%3D` keeps `>=` out of the URL, and the expression avoids `,`, which would split it.
URL="${CLICKHOUSE_URL}&query=SELECT+arrayJoin(range(1200%3E%3D1000))"
GLOB_URL="${CLICKHOUSE_URL}&query=SELECT+arrayJoin(range({0..1499}%3E%3D1000))"

echo "--- the schema of the address after the first batch goes into the cache"
$CLICKHOUSE_CLIENT --query "DESC url('$URL', 'TSV')"

echo "--- inference over the pattern takes it from there"
$CLICKHOUSE_CLIENT --query "DESC url('$GLOB_URL', 'TSV') SETTINGS glob_expansion_max_elements = 2000, engine_url_skip_empty_files = 1, schema_inference_cache_require_modification_time_for_url = 0, log_comment = '05044_url_glob_schema_cache'"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

echo "--- and the query log shows the cache hit"
$CLICKHOUSE_CLIENT --query "SELECT max(ProfileEvents['SchemaInferenceCacheHits']) > 0 FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '05044_url_glob_schema_cache' AND type = 'QueryFinish'"
