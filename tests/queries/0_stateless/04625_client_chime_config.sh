#!/usr/bin/env bash
# Tags: long
# `long` (matching 04312_client_chime_on_slow_query_92718, which uses the same
# `script -qc` pty mechanism) keeps this out of the `Fast test (arm_darwin)`
# run: BSD `script` on macOS does not honour the GNU `-qc "cmd" file` form, so
# the pty-attached client never runs and no `BEL` is captured there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Verify that <chime-threshold-seconds> in a client config file is respected,
# and that an explicit CLI --chime overrides it. This guards the !defaulted()
# check on --chime in ClientBase::addOptionsToTheClientConfiguration: without
# it, the CLI default (5) silently overwrites the value loaded from the config
# file, so a 1.5-second query would never chime with the configured 1-second
# threshold.
#
# The chime is the ASCII `BEL` character (`\x07`) on stderr, emitted only when
# stderr is attached to a terminal, so the client runs under `script -qc`
# which allocates a pty and captures its output to a file (the pattern of
# 04312_client_chime_on_slow_query_92718). `FORMAT Null` keeps stdout empty so
# only chime-related bytes appear in the captured stream.

config=$CLICKHOUSE_TMP/client_chime_${CLICKHOUSE_DATABASE}.xml
tty1="${CLICKHOUSE_TMP}/client_chime_tty1_${CLICKHOUSE_DATABASE}.txt"
tty2="${CLICKHOUSE_TMP}/client_chime_tty2_${CLICKHOUSE_DATABASE}.txt"

function cleanup()
{
    rm -f "${config}" "${tty1}" "${tty2}"
}
trap cleanup EXIT

cat > "$config" <<'EOF'
<config>
    <chime-threshold-seconds>1</chime-threshold-seconds>
</config>
EOF

bel_count() {
    # Print "BEL" if input file contains ASCII BEL (`\x07`), "no BEL" otherwise.
    if grep -q $'\x07' "$1"; then
        echo "BEL"
    else
        echo "no BEL"
    fi
}

# Case 1: threshold 1 from the config, no --chime flag, 1.5-second query on a
# pty — expect `BEL`. If the omitted CLI flag clobbered the config value with
# its default of 5 seconds, the query would stay below the threshold and no
# `BEL` would be emitted.
/usr/bin/script -qc "${CLICKHOUSE_CLIENT} --config '$config' -q 'SELECT sleep(1.5) FORMAT Null'" /dev/null > "$tty1" 2>&1
echo "1. chime-threshold-seconds=1 from config, no --chime, 1.5s query, pty stderr: $(bel_count "$tty1")"

# Case 2: threshold 1 from the config, explicit --chime 0 on the CLI, same
# query — the CLI must win over the config file, so no `BEL`.
/usr/bin/script -qc "${CLICKHOUSE_CLIENT} --config '$config' --chime 0 -q 'SELECT sleep(1.5) FORMAT Null'" /dev/null > "$tty2" 2>&1
echo "2. chime-threshold-seconds=1 from config, CLI --chime 0 wins, 1.5s query, pty stderr: $(bel_count "$tty2")"
