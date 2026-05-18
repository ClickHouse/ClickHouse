#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Pick the first output line that starts with the literal `1. ` (the row-numbered
# data line for the first row), then return the line bytes as a single hex string.
# Avoids loose `grep` matches against intermediate borders.
data_line_hex()
{
    LC_ALL=C awk 'substr($0, 1, 3) == "1. " { printf "%s", $0; exit }' \
        | od -An -v -tx1 \
        | tr -d ' \n'
}

run_pretty()
{
    local format=$1
    shift
    $CLICKHOUSE_CLIENT --output_format_pretty_color=0 "$@" \
        --query="SELECT toUInt64(7) AS number, 'x' AS label FORMAT ${format}"
}

# Expected byte building blocks.
N=c2a0           # `U+00A0` NO-BREAK SPACE in UTF-8
S=20             # ASCII space
BAR=e29482       # `│` U+2502
ROW_MARKER=312e20  # `1. `

# `PrettyCompact` data row, NBSP setting on:
#   1. │<1N leading><5N pad>7<1N trailing>│<1N leading>x<4N pad><1N trailing>│
expected_pc_nbsp="${ROW_MARKER}${BAR}${N}${N}${N}${N}${N}${N}37${N}${BAR}${N}78${N}${N}${N}${N}${N}${BAR}"
actual_pc_nbsp=$(run_pretty PrettyCompact --output_format_pretty_use_nbsp_for_padding=1 | data_line_hex)

# `PrettyCompact` data row, default: same shape, every space is `0x20`.
expected_pc_default="${ROW_MARKER}${BAR}${S}${S}${S}${S}${S}${S}37${S}${BAR}${S}78${S}${S}${S}${S}${S}${BAR}"
actual_pc_default=$(run_pretty PrettyCompact | data_line_hex)

# `Pretty` data row uses `│` for cells (the `┃` only appears in the bold header frame).
expected_p_nbsp="${ROW_MARKER}${BAR}${N}${N}${N}${N}${N}${N}37${N}${BAR}${N}78${N}${N}${N}${N}${N}${BAR}"
actual_p_nbsp=$(run_pretty Pretty --output_format_pretty_use_nbsp_for_padding=1 | data_line_hex)

# `PrettySpace` data row, NBSP setting on. No bars; cells join through padding.
#   1. <1N leading><5N pad>7<1N trailing><1N column-separator><1N leading>x<4N pad><1N trailing>
expected_ps_nbsp="${ROW_MARKER}${N}${N}${N}${N}${N}${N}37${N}${N}${N}78${N}${N}${N}${N}${N}"
actual_ps_nbsp=$(run_pretty PrettySpace --output_format_pretty_use_nbsp_for_padding=1 | data_line_hex)

# Row-number left padding: with `numbers(12)` single-digit rows need one leading
# pad before `1.`, `2.`, ..., `9.`. Line 2 of the output is the data row for `1.`;
# its first two bytes should be `U+00A0` when the setting is on.
single_digit_row_lead_hex=$(
    $CLICKHOUSE_CLIENT --output_format_pretty_color=0 \
        --output_format_pretty_use_nbsp_for_padding=1 \
        --query="SELECT number FROM numbers(12) FORMAT PrettyCompact" \
        | sed -n '2p' | head -c 2 | od -An -v -tx1 | tr -d ' \n'
)

# `PrettyCompact` under `ASCII` charset, setting on: NBSP must be suppressed.
ascii_charset_hex=$(run_pretty PrettyCompact \
    --output_format_pretty_use_nbsp_for_padding=1 \
    --output_format_pretty_grid_charset=ASCII \
    | data_line_hex)

[[ "$expected_pc_nbsp"     == "$actual_pc_nbsp"     ]] && echo "PrettyCompact NBSP data row matches expected bytes" || echo "PrettyCompact NBSP data row MISMATCH: $actual_pc_nbsp vs $expected_pc_nbsp"
[[ "$expected_pc_default"  == "$actual_pc_default"  ]] && echo "PrettyCompact default data row matches expected bytes" || echo "PrettyCompact default data row MISMATCH: $actual_pc_default vs $expected_pc_default"
[[ "$expected_p_nbsp"      == "$actual_p_nbsp"      ]] && echo "Pretty NBSP data row matches expected bytes"        || echo "Pretty NBSP data row MISMATCH: $actual_p_nbsp vs $expected_p_nbsp"
[[ "$expected_ps_nbsp"     == "$actual_ps_nbsp"     ]] && echo "PrettySpace NBSP data row matches expected bytes"   || echo "PrettySpace NBSP data row MISMATCH: $actual_ps_nbsp vs $expected_ps_nbsp"
[[ "$single_digit_row_lead_hex" == "$N" ]] && echo "Row-number left pad on single-digit row is NBSP" || echo "Row-number left pad MISMATCH: $single_digit_row_lead_hex"

case "$ascii_charset_hex" in
    *${N}*) echo "ASCII charset suppression FAILED (NBSP found)" ;;
    *)      echo "ASCII charset suppresses NBSP" ;;
esac
