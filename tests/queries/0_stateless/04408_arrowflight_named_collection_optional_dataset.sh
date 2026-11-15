#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A named collection that omits the optional `dataset` key must be accepted by ArrowFlight. It used to
# throw BAD_ARGUMENTS ("No such key 'dataset'") because processNamedCollectionResult overwrote the safe
# getOrDefault read with a bare get<String>("dataset"). With `dataset` omitted and basic auth disabled,
# arrowFlight now reaches the connection stage and fails with ARROWFLIGHT_FETCH_SCHEMA_ERROR (no server
# is listening) -- exactly as it would with `dataset` present -- proving the key is optional.
#
# Named collections are server-global, so the collection name is scoped to the (unique) test database
# to avoid collisions across repeated/concurrent runs (e.g. the flaky check).
NC="nc_arrowflight_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${NC}"
${CLICKHOUSE_CLIENT} --query "CREATE NAMED COLLECTION ${NC} AS host = 'localhost', port = 56789, use_basic_authentication = false"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM arrowFlight(${NC})" 2>&1 | grep -o -m1 "ARROWFLIGHT_FETCH_SCHEMA_ERROR"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${NC}"
