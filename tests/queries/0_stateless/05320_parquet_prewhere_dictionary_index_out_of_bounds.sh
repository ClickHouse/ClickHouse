#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dictionary index past the end of the dictionary must fail with INCORRECT_DATA when the column is read under PREWHERE.

# 60 rows, uncompressed, no page index: k = row number, s = 'v' || toString(k % 3) for k < 30, 'v' || toString(3 + k % 2) after;
# s has a 5-entry dictionary (v0..v4) with bit-packed indexes.
DATA='UEFSMRUEFeADFeADTBV4FQAAAAAAAAABAAAAAgAAAAMAAAAEAAAABQAAAAYAAAAHAAAACAAAAAkAAAAKAAAACwAAAAwAAAANAAAADgAAAA8AAAAQAAAAEQAAABIAAAATAAAAFAAAABUAAAAWAAAAFwAAABgAAAAZAAAAGgAAABsAAAAcAAAAHQAAAB4AAAAfAAAAIAAAACEAAAAiAAAAIwAAACQAAAAlAAAAJgAAACcAAAAoAAAAKQAAACoAAAArAAAALAAAAC0AAAAuAAAALwAAADAAAAAxAAAAMgAAADMAAAA0AAAANQAAADYAAAA3AAAAOAAAADkAAAA6AAAAOwAAABUAFWQVZCwVeBUQFQYVBhxYBDsAAAAYBAAAAAAREQAAAAYRQCAMRGEcSKIsTOM8UCRNVGVdWKZtXOd9YCiOZGmeaKqubOu+cCzPdG3feK7vAAAAJqwFHBUCGSUAEBkYAWsVABZ4FqQFFqQFJoYEJggcWAQ7AAAAGAQAAAAAEREAGSwVBBUAFQIAFQAVEBUCADwAAAAVBBU8FTxMFQoVAAAAAgAAAHYwAgAAAHYxAgAAAHYyAgAAAHYzAgAAAHY0FQAVNBU0LBV4FRAVBhUGHFgCdjQYAnYwEREAAAADEYgQIUKECBEiRIgQjeM4juM4juM4juMIACbyBxwVDBklABAZGAFzFQAWeBbEARbEASaEByauBhxYAnY0GAJ2MBERABksFQQVABUCABUAFRAVAgA8FvABAAAAFYACHBwAABwcAAAcHAAAACYBzjJlMSJKyoDgk+cElTQQIYyMtCMTRgA8XBfIKMOXjGVCoxNcKNEAmZRzLhoJoywpGUeQaKC4CfEQVp4oeQD7gDwARIiiPCA8o4kqBugQzBYljJnYwEAKJuYgEJuBxBXBYDKdEArH4sEkKB5YIFoyGgRHNzNoQGgSQqaAiajtFUAcHAAAHBwAABwcAAAAAaAgAgAAMBkBEMAAEJBAIKEQAAQgIASCAEQAkgQAkIAVBBk8SAZzY2hlbWEVBAAVAiUAGAFrJRZMrBMIEgAAABUMJQAYAXMlAEwcAAAAFngZHBksJqwFHBUCGSUAEBkYAWsVABZ4FqQFFqQFJoYEJggcWAQ7AAAAGAQAAAAAEREAGSwVBBUAFQIAFQAVEBUCABb0CBWgAhwAAAAm8gccFQwZJQAQGRgBcxUAFngWxAEWxAEmhAcmrgYcWAJ2NBgCdjAREQAZLBUEFQAVAgAVABUQFQIAFpQLFV4cFvABAAAAFugGFngmCBboBgAoS0NsaWNrSG91c2UgdmVyc2lvbiAyNi4xMC4xIChidWlsZCA2Y2ZhMTRjZDAzNzRiMzZkODU3M2RkNDhmZDY2MTMzNTJhYzA4NGU5KRksHAAAHAAAACIBAABQQVIx'

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
# Blocks of 10 rows, so the out-of-range rows below are read after the first block of the row group.
SETTINGS="input_format_parquet_max_block_size = 10, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_dictionary_filter_push_down = 0, use_query_condition_cache = 0"

${CLICKHOUSE_LOCAL} --query "SELECT base64Decode('$DATA') FORMAT RawBLOB" > "$FILE"
${CLICKHOUSE_LOCAL} --query "SELECT count(), countIf(s = 'v3'), countIf(s = 'v4') FROM file('$FILE') PREWHERE k % 7 = 3 SETTINGS $SETTINGS"

# Dictionary page header patched to declare 4 entries instead of 5, so v4 (first passing row: k = 31) is out of range.
${CLICKHOUSE_LOCAL} --query "SELECT overlay(base64Decode('$DATA'), '\\x08', 416) FORMAT RawBLOB" > "$FILE"
${CLICKHOUSE_LOCAL} --query "SELECT count(), countIf(s = 'v3') FROM file('$FILE') PREWHERE k % 7 = 3 SETTINGS $SETTINGS" 2>&1 \
    | grep -o -m1 'Dict index or rep/def level out of bounds (bp)'

rm -f "$FILE"
