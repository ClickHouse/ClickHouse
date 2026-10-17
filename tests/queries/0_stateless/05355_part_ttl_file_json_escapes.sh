#!/usr/bin/env bash

# `JSON::getString` (base/base/JSON.cpp) decodes the string values of a data part's `ttl.txt`:
# `MergeTreeDataPartTTLInfos::read` takes the column name and the move-TTL expression through it, and
# `IMergeTreeDataPart::loadTTLInfos` reports a failure as `BAD_TTL_FILE`. ClickHouse writes that file
# with `doubleQuoteString`, which only ever emits `\"`, `\\`, `\b`, `\f`, `\n`, `\r` and `\t`, so the
# `\/` and `\uXXXX` escapes that JSON also allows, unknown escapes, and malformed strings are only
# reachable through a hand-written file. The decoded move-TTL expression is what
# `system.parts.move_ttl_info.expression` reports (docs/en/operations/system-tables/parts.md).
# `clickhouse local` is used because the test rewrites a file inside a detached part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${workdir:?}"
mkdir -p "${workdir}"

# The directory name of the part with block number $1.
part_name() { echo "all_${1}_${1}_0"; }

# A column TTL is what makes a part write `ttl.txt` at all. The column name is long on purpose:
# a part file is checked against the size recorded in `checksums.txt`, so the rewritten TTL file has
# to fit into the size of the original one.
${CLICKHOUSE_LOCAL} --path "${workdir}" -m -q "
CREATE TABLE tab (d DateTime, column_with_a_ttl_that_makes_the_ttl_file_long UInt64 TTL d + INTERVAL 100 YEAR)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO tab VALUES ('2000-01-01 00:00:00', 1);
ALTER TABLE tab DETACH PART '$(part_name 1)';
"

detached=$(find "${workdir}/store" -type d -name detached | head -1)
for n in 2 3 4; do cp -r "${detached:?}/$(part_name 1)" "${detached}/$(part_name "${n}")"; done
# The files of a data part are created read-only, and `cp` copies those permission bits, so the
# detached parts have to be made writable before their TTL file can be rewritten.
chmod -R u+w "${detached:?}"

header='ttl format version: 1'
ttl_size=$(wc -c < "${detached}/$(part_name 1)/ttl.txt")

# The TTL file is read before the file sizes of the part are checked, so a body that the decoder
# rejects does not have to keep the original size. `columns` is the only key of the object and the
# broken value is the last one inside it, so the decoder is the first thing that looks at it.
write_ttl() { printf '%s\n%s' "${header}" "$2" > "${detached}/$1/ttl.txt"; }

write_ttl "$(part_name 1)" '{"columns":[{"min":1,"max":2,"finished":0,"name":123}]}'
write_ttl "$(part_name 2)" '{"columns":[{"min":1,"max":2,"finished":0,"name":"never closed'
write_ttl "$(part_name 3)" '{"columns":[{"min":1,"max":2,"finished":0,"name":"A\uZZZZB"}]}'

attach_broken() {
    echo -n "$1 "
    ${CLICKHOUSE_LOCAL} --path "${workdir}" -q "ALTER TABLE tab ATTACH PART '$2'" 2>&1 | grep -o BAD_TTL_FILE
}

attach_broken 'the value is not a string:' "$(part_name 1)"
attach_broken 'the string is never closed:' "$(part_name 2)"
attach_broken 'no hex digits after backslash-u:' "$(part_name 3)"

# Every escape the decoder knows, in the order it tries them. Only the escapes that JSON documents
# are used here: an unknown escape is malformed input, for which `base/base/JSON.h` promises no
# result. `\u0416` is CYRILLIC CAPITAL LETTER ZHE, two bytes in UTF-8. The decoded value is padded
# with `x` to keep the TTL file at its original size; the padding is not part of the check, only the
# first 20 bytes are, which is what the escapes above decode to.
escapes='{"moves":[{"min":1,"max":2,"finished":0,"expression":"A\"B\\C\/D\bE\fF\nG\rH\tI\u0416J'
closing='"}]}'
padding=$(( ttl_size - ${#header} - 1 - ${#escapes} - ${#closing} ))
{
    printf '%s\n%s' "${header}" "${escapes}"
    printf "%${padding}s" '' | tr ' ' x
    printf '%s' "${closing}"
} > "${detached}/$(part_name 4)/ttl.txt"

${CLICKHOUSE_LOCAL} --path "${workdir}" -m -q "
SYSTEM STOP MERGES tab;
ALTER TABLE tab ATTACH PART '$(part_name 4)';
SELECT 'rows in the table', count() FROM tab;
SELECT 'decoded escapes', hex(substring(move_ttl_info.expression[1], 1, 20))
FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active;
SELECT 'parts left in detached', count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 'tab';
"

rm -rf "${workdir:?}"
