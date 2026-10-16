#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

exact_function="${CLICKHOUSE_DATABASE}_division_exact_udf"
legacy_function="${CLICKHOUSE_DATABASE}_division_legacy_udf"

cleanup()
{
    $CLICKHOUSE_CLIENT --multiquery --query "DROP FUNCTION IF EXISTS ${exact_function}; DROP FUNCTION IF EXISTS ${legacy_function};"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --multiquery --query "
SET exact_division_for_integers = 1;
CREATE FUNCTION ${exact_function} AS x -> x / 2;
SET exact_division_for_integers = 0;
SELECT ${exact_function}(toInt64(5));
CREATE FUNCTION ${legacy_function} AS x -> x / 2;
SET exact_division_for_integers = 1;
SELECT ${legacy_function}(toInt64(5));
"
