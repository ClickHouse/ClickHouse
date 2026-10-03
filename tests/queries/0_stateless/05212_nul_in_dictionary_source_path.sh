#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for a user_files sandbox bypass in dictionary SOURCE paths.
# The path check validated the length-aware std::string, but the file was later
# opened through a NUL-terminated C string. A PATH literal like
# `/etc/passwd\0/../../user_files/guard` therefore passed the confinement check via
# lexical `..` cancellation and then opened `/etc/passwd`, outside user_files.
# The validators must reject any path containing an embedded NUL.

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY IF EXISTS ${CLICKHOUSE_DATABASE}.nul_path_probe"
}
trap cleanup EXIT

# The NUL truncates the path at the syscall to `/etc/passwd`, while the trailing
# `../..` cancels it lexically to land inside user_files during validation.
# With the fix the embedded NUL is rejected outright: /etc/passwd must not be read.
# The check may fire at CREATE or at load time depending on lazy loading, so both
# statements are run and their combined output is inspected. grep -a keeps grep in
# text mode even if the target file's bytes leak into the output.
echo "--- embedded NUL in dictionary path is rejected ---"
{
    ${CLICKHOUSE_CLIENT} --query "
        CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.nul_path_probe
        (regexp String, value String)
        PRIMARY KEY regexp
        SOURCE(YAMLRegExpTree(PATH '/etc/passwd\0/../../user_files/guard'))
        LAYOUT(regexp_tree) LIFETIME(0)"
    ${CLICKHOUSE_CLIENT} --query "SELECT dictGet('${CLICKHOUSE_DATABASE}.nul_path_probe', 'value', 'anything')"
} 2>&1 | grep -a -o -m1 "is not inside\|root:" || echo "UNEXPECTED"
