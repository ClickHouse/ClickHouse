#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The stripe footer of this blob is truncated, so ORC's decompression stream reports the
# failure by throwing from inside the protobuf parser. The parse must surface as a regular
# INCORRECT_DATA exception; a protobuf has-bits assertion instead means the message was left
# inconsistent while the exception unwound. Only debug and sanitizer builds check that
# assertion, so the discriminating arm of this test is those builds.
BLOB='4f52431100000a061204080150005500000a280a06000000000000121e08014a1818b8fef997d7d63820b8fef997d7d63828c1fc1530c1fc1550000f0000780039604d293e0b00006e0007dda36300f00a0608061000180b0a0608061001182d0a0608011001180a0a060805100118081204080010001204080210001a03474d545100000a260a04080150000a1e08014a1818b8fef997d7d63820b8fef997d7d63828c1fc1530c1fc155000b500000803107e1a0a08031038181220342801220f080c1201011a0263302000280030002208080920002800300030013a04080150003a1e08014a1818b8fef997d7d63820b8fef997d7d63828c1fc1530c1fc15500040904e48016200085d1005188080102202000c282b300682f403034f524317'

# The fixture must stay pure even-length lowercase hex: unhex's result for a non-hex character is
# implementation-defined, so one invalid character makes the corrupt footer decoder-dependent and
# this test silently stops asserting anything.
if [[ ! $BLOB =~ ^[0-9a-f]+$ ]] || (( ${#BLOB} % 2 )); then
    echo "BLOB must be an even-length string of [0-9a-f]" >&2
    exit 1
fi

$CLICKHOUSE_LOCAL -q "SELECT c0 FROM format(ORC, 'c0 DateTime64(3)', unhex('$BLOB'));" 2>&1 \
    | grep -oE "Code: [0-9]+.*Read past EOF in DecompressionStream::readBuffer|INCORRECT_DATA|Has bits mismatch|Check failure stack trace" \
    | sed -E 's/^Code: [0-9]+.*Read past EOF in DecompressionStream::readBuffer$/Read past EOF in DecompressionStream::readBuffer/' \
    | sort -u

# Well-formed ORC must still read back unchanged.
DATA_FILE="$CLICKHOUSE_TMP/04856_orc_control_${CLICKHOUSE_DATABASE}.orc"
$CLICKHOUSE_LOCAL -q "SELECT number AS a, toString(number) AS b FROM numbers(3) FORMAT ORC" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "SELECT a, b FROM file('$DATA_FILE', ORC) ORDER BY a"

rm "$DATA_FILE"
