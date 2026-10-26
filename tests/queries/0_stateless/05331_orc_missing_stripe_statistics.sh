#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in the fast test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An ORC file (id Int64, 10 rows, one stripe) whose Metadata section has no stripe statistics, as when a corrupt
# PostScript `metadata_length` makes it parse short. Filter pushdown must read the stripe as one without statistics.
# The predicate must keep the file (`id > 5`), or the file statistics skip it before the stripe statistics are read.
# The second file has two stripes and a too-small `metadata_length`, so only the second stripe's statistics remain;
# they must not be applied to the first stripe.
# `indexHint` counts the rows read: the row group index must still skip the second stripe of that file.
ORC_B64=$(tr -d '\n' <<'B64'
T1JDCgYSBAgKUAAKEgoCAAASDAgKEgYIABASGFpQAMAJAAIKBggGEAAYCAoGCAYQARgUCgYIARABGAQSBAgAEAASBAgCEAAaA0dN
VBAACAMQSRoKCAMQHBgEICkoCiIPCAwSAQEaAmlkIAAoADAAIggIBCAAKAAwADAKOgQIClAAOgwIChIGCAAQEhhaUABAkE5IAWIA
CEgQABiAgBAiAgAMKAIwBoL0AwNPUkMX
B64
)
FILE="${CLICKHOUSE_DATABASE}_missing_stripe_statistics.orc"
$CLICKHOUSE_CLIENT --engine_file_truncate_on_insert 1 \
    --query "INSERT INTO FUNCTION file('$FILE', 'RawBLOB') SELECT base64Decode('$ORC_B64')"
for pushdown in 1 0; do
    $CLICKHOUSE_CLIENT --input_format_orc_filter_push_down $pushdown \
        --query "SELECT count(), sum(id) FROM file('$FILE', 'ORC') WHERE id > 5"
done

ORC_B64_2=$(tr -d '\n' <<'B64'
T1JDCgYSBAgKUAAKEgoCAAASDAgKEgYIABASGFpQAMAJAAIKBggGEAAYCAoGCAYQARgUCgYIARABGAQSBAgAEAASBAgCEAAaA0dN
VAoGEgQIClAAChMKAgAAEg0IChIHCBQQJhiiAlAAwAkUAgoGCAYQABgICgYIBhABGBUKBggBEAEYBBIECAAQABIECAIQABoDR01U
ChQKBAgKUAAKDAgKEgYIABASGFpQAAoVCgQIClAACg0IChIHCBQQJhiiAlAACAMQkwEaCggDEBwYBCApKAoaCghMEB0YBCApKAoi
DwgMEgEBGgJpZCAAKAAwACIICAQgACgAMAAwFDoECBRQADoNCBQSBwgAECYY/AJQAEAKSAFiAAhVEAAYgIAEIgIADCgXMAaC9AMD
T1JDFw==
B64
)
FILE2="${CLICKHOUSE_DATABASE}_partial_stripe_statistics.orc"
$CLICKHOUSE_CLIENT --engine_file_truncate_on_insert 1 \
    --query "INSERT INTO FUNCTION file('$FILE2', 'RawBLOB') SELECT base64Decode('$ORC_B64_2')"
for pushdown in 1 0; do
    $CLICKHOUSE_CLIENT --input_format_orc_filter_push_down $pushdown \
        --query "SELECT count(), sum(id) FROM file('$FILE2', 'ORC') WHERE id < 5"
done
for pushdown in 1 0; do
    $CLICKHOUSE_CLIENT --input_format_orc_filter_push_down $pushdown \
        --query "SELECT count(), sum(id) FROM file('$FILE2', 'ORC') WHERE indexHint(id < 5)"
done
rm -f "${USER_FILES_PATH:?}/$FILE" "${USER_FILES_PATH:?}/$FILE2"
