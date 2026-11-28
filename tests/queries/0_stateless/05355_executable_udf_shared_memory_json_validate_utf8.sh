#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# The input of a shared-memory UDF is serialized into the region once, also by a format that writes
# through a buffer wrapped around the region's (`WriteBufferValidUTF8`, with
# `output_format_json_validate_utf8`): the command answers every row it is given, so a row written
# twice comes back twice and fails the query with "wrong result".

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

{
    echo "<function><type>executable</type><name>shm_json_echo</name><return_type>String</return_type><return_name>value</return_name>"
    echo "<argument><name>value</name><type>String</type></argument><format>JSONEachRow</format>"
    echo "<use_shared_memory>1</use_shared_memory><shared_memory_size>4096</shared_memory_size><command>shm_udf.py --echo</command></function>"
} | shm_functions

shm_local "
    SELECT shm_json_echo('hello') SETTINGS output_format_json_validate_utf8 = 1;
    SELECT shm_json_echo(toString(number)) FROM numbers(3) SETTINGS output_format_json_validate_utf8 = 1;
    SELECT shm_json_echo('hello') SETTINGS output_format_json_validate_utf8 = 0;
"
