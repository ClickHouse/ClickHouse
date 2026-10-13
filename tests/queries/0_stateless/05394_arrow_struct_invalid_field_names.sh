#!/usr/bin/env bash
# Tags: no-fasttest
# Arrow struct fields with empty or repeated names are read as an unnamed `Tuple`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE="$CURDIR/data_arrow/struct_invalid_field_names.arrow"
STREAM="$CURDIR/data_arrow/struct_invalid_field_names.arrows"

$CLICKHOUSE_LOCAL -q "
    DESC file('$FILE', Arrow);
    SELECT * FROM file('$FILE', Arrow) ORDER BY id;
    DESC file('$STREAM', ArrowStream);
    SELECT * FROM file('$STREAM', ArrowStream) ORDER BY id;
    SELECT tupleElement(s_empty, 1), tupleElement(s_empty, 2), tupleElement(s_dup, 2) FROM file('$FILE', Arrow) ORDER BY id;
    SELECT s_empty, s_dup FROM file('$FILE', Arrow, 'id Int32, s_empty Tuple(Nullable(Int32), Nullable(String)), s_dup Tuple(Int32, String)') ORDER BY id;
"
