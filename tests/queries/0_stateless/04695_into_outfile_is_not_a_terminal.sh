#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin - darwin does not support "script -qc"

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# INTO OUTFILE writes to a file even when stdout is a terminal, so with the
# default output_format_pretty_color=auto the file must not contain ANSI
# escape sequences. The client is run under a pty (via script) to make its
# stdout a terminal.

out_file="${CLICKHOUSE_TMP}/04695_out_${CLICKHOUSE_DATABASE}.txt"
tty_out="${CLICKHOUSE_TMP}/04695_tty_${CLICKHOUSE_DATABASE}.txt"

esc_check() {
    if grep -q $'\x1b' "$1"; then
        echo "escapes"
    else
        echo "no escapes"
    fi
}

for format in Vertical PrettyCompact; do
    rm -f "$out_file"
    script -eqc "${CLICKHOUSE_CLIENT} -q \"SELECT 1 AS x INTO OUTFILE '$out_file' FORMAT $format\"" /dev/null > /dev/null 2>&1
    echo "$format INTO OUTFILE, pty stdout: $(esc_check "$out_file")"

    rm -f "$out_file"
    script -eqc "${CLICKHOUSE_CLIENT} -q \"SELECT 1 AS x INTO OUTFILE '$out_file' AND STDOUT FORMAT $format\"" /dev/null > /dev/null 2>&1
    echo "$format INTO OUTFILE AND STDOUT, pty stdout: $(esc_check "$out_file")"

    # control: the same query without INTO OUTFILE must be colored on the pty,
    # otherwise the cases above pass vacuously
    script -eqc "${CLICKHOUSE_CLIENT} -q 'SELECT 1 AS x FORMAT $format'" /dev/null > "$tty_out" 2>&1
    echo "$format to pty stdout: $(esc_check "$tty_out")"
done

rm -f "$out_file" "$tty_out"
