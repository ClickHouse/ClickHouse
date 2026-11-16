#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

nc="${CLICKHOUSE_DATABASE}"

trap '${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${nc}"' EXIT

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${nc}"

# OR REPLACE creates the collection when it does not exist
${CLICKHOUSE_CLIENT} -q "CREATE OR REPLACE NAMED COLLECTION ${nc} AS a = '1', b = '2' OVERRIDABLE"
${CLICKHOUSE_CLIENT} -q "SELECT create_query FROM system.named_collections WHERE name = '${nc}'"

# plain CREATE still throws, IF NOT EXISTS still no-ops
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${nc} AS c = '3' -- {serverError NAMED_COLLECTION_ALREADY_EXISTS}"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION IF NOT EXISTS ${nc} AS c = '3'"
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(mapKeys(collection)) FROM system.named_collections WHERE name = '${nc}'"

# full replacement: key removed, key added, flag switched
${CLICKHOUSE_CLIENT} -q "CREATE OR REPLACE NAMED COLLECTION ${nc} AS a = '10', c = '3' NOT OVERRIDABLE"
${CLICKHOUSE_CLIENT} -q "SELECT create_query FROM system.named_collections WHERE name = '${nc}'"

# flag cleared on the only key, which is not expressible via ALTER
${CLICKHOUSE_CLIENT} -q "CREATE OR REPLACE NAMED COLLECTION ${nc} AS c = '3'"
${CLICKHOUSE_CLIENT} -q "SELECT create_query FROM system.named_collections WHERE name = '${nc}'"

# OR REPLACE and IF NOT EXISTS cannot be combined
${CLICKHOUSE_CLIENT} -q "CREATE OR REPLACE NAMED COLLECTION IF NOT EXISTS ${nc} AS a = '1' -- {clientError SYNTAX_ERROR}"

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${nc}"

