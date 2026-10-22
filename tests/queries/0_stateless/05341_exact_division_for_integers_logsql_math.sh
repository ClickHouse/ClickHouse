#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE TABLE logs_exact_division_math (_time DateTime, _msg String, a Int64, b Nullable(UInt64)) ENGINE = Memory;
    INSERT INTO logs_exact_division_math VALUES (0, 'one', 1, 2), (0, 'two', 4, NULL), (0, 'three', -1, 3);
"

for exact in 0 1; do
    for query in \
        '* | math 1 / 2 as result | fields result | limit 1' \
        '* | math -3 / 2 as result | fields result | limit 1' \
        '* | math 1 / 2 default 9 as result | fields result | limit 1' \
        '* | math 1 / 0 default 42 as result | fields result | limit 1' \
        '* | math -1 / 0 default 42 as result | fields result | limit 1' \
        '* | math 0 / 0 default 42 as result | fields result | limit 1' \
        '* | math round(1500, 1000) as result | fields result | limit 1' \
        '* | stats count() as c, count() if (a:>0) as errors | math errors / c as result | fields result' \
        '* | math a / b as result | sort by (a) | fields result'; do
        $CLICKHOUSE_CLIENT --enable_logsql_dialect 1 --logsql_table logs_exact_division_math --dialect logsql \
            --exact_division_for_integers "$exact" --use_variant_as_common_type 1 \
            --format TabSeparatedWithNamesAndTypes --query "$query"
    done
done

$CLICKHOUSE_CLIENT --query "DROP TABLE logs_exact_division_math"
