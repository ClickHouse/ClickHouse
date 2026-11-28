#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `num_iterations` layout parameter must be a positive integer that fits the `int` iteration index of
# XGBoost. A value that is not an integer and a positive integer that is too large are reported differently,
# so the error message is checked, not only the error code.

$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS training_05260;
CREATE TABLE training_05260 (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_05260 SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);
"

for value in "'abc'" "'10abc'" "1.5" "0" "2147483647" "2147483648" "4000000000"
do
    echo "num_iterations ${value}"
    # The dictionary loads on the first `predictXGBoost` call, which is where the parameter is validated.
    # 2147483647 is accepted by the parsing, so it is paired with an unknown parameter to stop before training.
    extra=""
    if [ "${value}" = "2147483647" ]; then
        extra="not_a_training_param 1"
    fi
    $CLICKHOUSE_CLIENT --enable_xgboost=1 --multiquery "
    DROP DICTIONARY IF EXISTS model_05260;
    CREATE DICTIONARY model_05260 (x1 Float64, x2 Float64, y Float64)
    PRIMARY KEY (x1, x2) SOURCE(CLICKHOUSE(TABLE 'training_05260'))
    LAYOUT(XGBOOST(${extra} num_iterations ${value})) LIFETIME(0);
    SELECT predictXGBoost('model_05260', 1.0, 2.0);
    " 2>&1 | grep -o -E "BAD_ARGUMENTS|must be a positive integer, got '[^']*'|is [0-9]+, but the maximum is [0-9]+|Unknown or forbidden training parameter '[^']*'" | sort -u
done

$CLICKHOUSE_CLIENT --multiquery "
DROP DICTIONARY IF EXISTS model_05260;
DROP TABLE training_05260;
"
