#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table created as a table function keeps a dependency on the named collection the table function uses.
# Named collections are global, so the name has to be unique across concurrently running tests.

NC="nc_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS json_str = '{}';
CREATE TABLE 04903_table AS fuzzJSON(${NC});
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE 04903_table;
DROP NAMED COLLECTION ${NC};
"
