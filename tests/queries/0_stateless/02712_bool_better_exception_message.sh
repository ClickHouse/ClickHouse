#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_LOCAL <<END
    SELECT * FROM format(JSONEachRow, 'x Bool', '{"x": 1}');
    SELECT * FROM format(JSONEachRow, 'x Bool', '{"x": null}');
END

$CLICKHOUSE_LOCAL <<END 2>&1 | rg -Fc "'w' character"
    SELECT * FROM format(JSONEachRow, 'x Bool', '{"x": wtf}');
END

$CLICKHOUSE_LOCAL <<END 2>&1 | rg -Fc "expected 'false'"
    SELECT * FROM format(JSONEachRow, 'x Bool', '{"x": ftw}');
END

$CLICKHOUSE_LOCAL <<END 2>&1 | rg -Fc "'{' character"
    SELECT * FROM format(JSONEachRow, 'x Bool', '{"x": {}}');
END
