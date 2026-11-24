#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Schema inference of `url` counts the failover options (`|`) of every address against the limit, as
# reading does, not only the addresses separated by `,`: `SELECT+{1,2}{1|2}` is two addresses with two
# replicas each, four in total. Inference stops at the first address it can read from, so the limit is
# only hit when the addresses within it are all empty. The addresses are served by the HTTP interface
# of the server the test runs against. Parallel replicas may rewrite `url` into its cluster
# counterpart, which changes the surface, so pin the plain code path.

URL="${CLICKHOUSE_URL}&query=SELECT+{1,2}{1|2}"
EMPTY_URL="${CLICKHOUSE_URL}&query=SELECT+{1,2}{1|2}+WHERE+0"

# The first address is readable, so inference never looks past it.
$CLICKHOUSE_CLIENT --query "DESC url('$URL', TSV) SETTINGS glob_expansion_max_elements = 3, enable_parallel_replicas = 0, schema_inference_use_cache_for_url = 0"

# Both replicas of the first address are within the limit, the second address is not.
$CLICKHOUSE_CLIENT --query "DESC url('$EMPTY_URL', TSV) SETTINGS glob_expansion_max_elements = 3, enable_parallel_replicas = 0, schema_inference_use_cache_for_url = 0" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 4, while at most 3 are allowed" \
    | head -n 2

# With room for all four addresses, inference reads both and finds them empty.
$CLICKHOUSE_CLIENT --query "DESC url('$EMPTY_URL', TSV) SETTINGS glob_expansion_max_elements = 4, enable_parallel_replicas = 0, schema_inference_use_cache_for_url = 0" 2>&1 \
    | grep -oF -e "too many result addresses" -e "the file is empty" \
    | head -n 1
