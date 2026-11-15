#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE TABLE logs_exact_division (_time DateTime, _msg String, a Int64, b Nullable(UInt64)) ENGINE = Memory;
    INSERT INTO logs_exact_division VALUES (0, 'one', 1, 2), (0, 'two', 4, NULL), (0, 'three', -1, 3);
"

# Check both values and types: the integer default in `ifNull` can introduce a `Variant` alternative.
for exact in 0 1; do
    for query in '* | stats avg(a, b) as mean' '* | stats avg(b, b) as mean' 'a:>999 | stats avg(a, b) as mean'; do
        $CLICKHOUSE_CLIENT --enable_logsql_dialect 1 --logsql_table logs_exact_division --dialect logsql \
            --sql_compatible_division_for_integers "$exact" --use_variant_as_common_type 1 \
            --format TabSeparatedWithNamesAndTypes --query "$query"
    done
done

$CLICKHOUSE_CLIENT --query "DROP TABLE logs_exact_division"
