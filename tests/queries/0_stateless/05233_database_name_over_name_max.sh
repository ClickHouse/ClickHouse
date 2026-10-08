#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The file-backed database engines resolve a table name into a local path and probe it with the
# throwing `std::filesystem` overloads, so a name longer than the filesystem's NAME_MAX escaped as
# `STD_EXCEPTION` instead of resolving to "no such table". Every arm greps for `STD_EXCEPTION` too,
# so a regression prints it into the output and the reference mismatches.

pad() { printf '%*s' "$1" '' | tr ' ' a; }
LONG_NAME=$(pad 300)

echo "--- local: a table name over NAME_MAX is an unknown table ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM ${LONG_NAME}" 2>&1 | grep -o -m1 -E 'UNKNOWN_TABLE|STD_EXCEPTION'
echo "--- local: control, a short unknown table name ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM t_05233_absent" 2>&1 | grep -o -m1 -E 'UNKNOWN_TABLE|STD_EXCEPTION'

echo "--- local: a dictionary name over NAME_MAX is not found ---"
dict_err=$(${CLICKHOUSE_LOCAL} -q "SELECT dictGetFloat64(concat('PT', repeat('9', 309)), 'x', 1)" 2>&1)
echo "$dict_err" | grep -o -m1 -E 'BAD_ARGUMENTS|STD_EXCEPTION'
if echo "$dict_err" | grep -qE 'Dictionary \(.+\) not found'; then echo "dictionary-not-found"; else echo "OTHER_MESSAGE"; fi
echo "--- local: control, a short unknown dictionary name ---"
dict_err=$(${CLICKHOUSE_LOCAL} -q "SELECT dictGetFloat64('PT9', 'x', 1)" 2>&1)
echo "$dict_err" | grep -o -m1 -E 'BAD_ARGUMENTS|STD_EXCEPTION'
if echo "$dict_err" | grep -qE 'Dictionary \(.+\) not found'; then echo "dictionary-not-found"; else echo "OTHER_MESSAGE"; fi

echo "--- local: EXISTS TABLE over NAME_MAX answers 0 ---"
${CLICKHOUSE_LOCAL} -q "EXISTS TABLE ${LONG_NAME}" 2>&1 | grep -o -m1 -E '^0$|^1$|STD_EXCEPTION'

# The server reaches the same probe through a `Filesystem` database.
db_dir="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${db_dir}"
mkdir -p "${db_dir}/subdir"
printf '1,2\n3,4\n' > "${db_dir}/tmp.csv"

DB_FS="${CLICKHOUSE_DATABASE}_fs"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB_FS}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB_FS} ENGINE = Filesystem('${CLICKHOUSE_TEST_UNIQUE_NAME}')"

echo "--- server: a table name over NAME_MAX is an unknown table ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`${LONG_NAME}\`" 2>&1 | grep -o -m1 -E 'UNKNOWN_TABLE|STD_EXCEPTION'
echo "--- server: EXISTS TABLE over NAME_MAX answers 0 ---"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB_FS}.\`${LONG_NAME}\`" 2>&1 | grep -o -m1 -E '^0$|^1$|STD_EXCEPTION'
echo "--- server: a real file in the database still resolves ---"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB_FS}.\`tmp.csv\`" 2>&1 | grep -o -m1 -E '^[0-9]+$|STD_EXCEPTION'
echo "--- server: a directory is not a table ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`subdir\`" 2>&1 | grep -o -m1 -E 'UNKNOWN_TABLE|STD_EXCEPTION'
echo "--- server: a dictionary name over NAME_MAX is not found ---"
dict_err=$(${CLICKHOUSE_CLIENT} -q "SELECT dictGetFloat64('${DB_FS}.${LONG_NAME}', 'x', 1)" 2>&1)
echo "$dict_err" | grep -o -m1 -E 'BAD_ARGUMENTS|STD_EXCEPTION'
if echo "$dict_err" | grep -qE 'Dictionary \(.+\) not found'; then echo "dictionary-not-found"; else echo "OTHER_MESSAGE"; fi

# A globbed name skips the existence probe entirely, so it is cached under a literal path the
# filesystem cannot stat; resolving it a second time went through the cache probe and threw.
# Brace alternation rather than 300 `*`, which would compile to 300 nested `[^/]*`.
GLOB_NAME="{tmp.csv,$(pad 300)}"
echo "--- server: a globbed name over NAME_MAX resolves and is cached ---"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB_FS}.\`${GLOB_NAME}\`" 2>&1 | grep -o -m1 -E '^[0-9]+$|STD_EXCEPTION'
echo "--- server: and resolves again, through the cache probe ---"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${DB_FS}.\`${GLOB_NAME}\`" 2>&1 | grep -o -m1 -E '^[0-9]+$|STD_EXCEPTION'
echo "--- server: EXISTS TABLE on the cached globbed name ---"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB_FS}.\`${GLOB_NAME}\`" 2>&1 | grep -o -m1 -E '^0$|^1$|STD_EXCEPTION'

# A glob after an over-long path component leaves that component in the literal prefix
# `StorageFile` probes, so the probe threw instead of reporting that nothing matches.
echo "--- server: a glob under an over-long directory matches nothing ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`${LONG_NAME}/*.csv\`" 2>&1 | grep -o -m1 -E 'CANNOT_EXTRACT_TABLE_STRUCTURE|STD_EXCEPTION'
echo "--- server: control, a glob under a short absent directory ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`t_05233_absent_dir/*.csv\`" 2>&1 | grep -o -m1 -E 'CANNOT_EXTRACT_TABLE_STRUCTURE|STD_EXCEPTION'

# `{_partition_id}` is the partitioned-write placeholder, and its braces also make the name look
# globbed to the database guards, so the name reaches the write-path check verbatim and is stated
# on the filesystem there. Server only: that check is skipped outside a server.
echo "--- server: a name over NAME_MAX with the partition wildcard is not a filesystem error ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`${LONG_NAME}{_partition_id}\`" 2>&1 | grep -o -m1 -E 'CANNOT_STAT|STD_EXCEPTION'
echo "--- server: control, a short name with the partition wildcard ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`t_05233_absent{_partition_id}\`" 2>&1 | grep -o -m1 -E 'CANNOT_STAT|STD_EXCEPTION'

# Archive path syntax splits `<archive>::<member>` and carries only the archive component into the
# path listing, so the member glob that made this name look globbed to the database guards is no
# longer part of the string being stated.
echo "--- server: a name over NAME_MAX with archive syntax is not a filesystem error ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`${LONG_NAME}.zip::*\`" 2>&1 | grep -o -m1 -E 'CANNOT_STAT|STD_EXCEPTION'
echo "--- server: control, a short name with archive syntax ---"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${DB_FS}.\`t_05233_absent.zip::*\`" 2>&1 | grep -o -m1 -E 'CANNOT_STAT|STD_EXCEPTION'

echo "--- local: a glob under an over-long directory matches nothing ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM \`${LONG_NAME}/*.csv\`" 2>&1 | grep -o -m1 -E 'CANNOT_EXTRACT_TABLE_STRUCTURE|STD_EXCEPTION'
echo "--- local: control, a glob under a short absent directory ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM \`t_05233_absent_dir/*.csv\`" 2>&1 | grep -o -m1 -E 'CANNOT_EXTRACT_TABLE_STRUCTURE|STD_EXCEPTION'

echo "--- local: a name over NAME_MAX with archive syntax is not a filesystem error ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM \`${LONG_NAME}.zip::*\`" 2>&1 | grep -o -m1 -E 'CANNOT_STAT|STD_EXCEPTION'
echo "--- local: control, a short name with archive syntax ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM \`t_05233_absent.zip::*\`" 2>&1 | grep -o -m1 -E 'CANNOT_STAT|STD_EXCEPTION'

# A path written out in `file()` reaches the same non-globbed listing probe as a bare name.
echo "--- local: file() on a written-out path over NAME_MAX ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${LONG_NAME}')" 2>&1 | grep -o -m1 -E 'CANNOT_STAT|STD_EXCEPTION'
echo "--- local: control, file() on a short absent path ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('t_05233_absent')" 2>&1 | grep -o -m1 -E 'CANNOT_STAT|STD_EXCEPTION'

# An explicit structure skips schema inference, so the path is probed again when the source opens
# it: once for a plain path, and once per archive on the two archive-reading branches.
echo "--- local: file() over NAME_MAX with an explicit structure ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${LONG_NAME}', 'CSV', 'x Int32')" 2>&1 | grep -o -m1 -E 'FILE_DOESNT_EXIST|STD_EXCEPTION'
echo "--- local: control, file() on a short absent path with an explicit structure ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('t_05233_absent', 'CSV', 'x Int32')" 2>&1 | grep -o -m1 -E 'FILE_DOESNT_EXIST|STD_EXCEPTION'
echo "--- local: archive over NAME_MAX with a member glob and an explicit structure ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${LONG_NAME}.zip::*', 'CSV', 'x Int32')" 2>&1 | grep -o -m1 -E 'FILE_DOESNT_EXIST|STD_EXCEPTION'
echo "--- local: control, a short archive with a member glob and an explicit structure ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('t_05233_absent.zip::*', 'CSV', 'x Int32')" 2>&1 | grep -o -m1 -E 'FILE_DOESNT_EXIST|STD_EXCEPTION'
echo "--- local: archive over NAME_MAX with a named member and an explicit structure ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${LONG_NAME}.zip::inner.csv', 'CSV', 'x Int32')" 2>&1 | grep -o -m1 -E 'FILE_DOESNT_EXIST|STD_EXCEPTION'
echo "--- local: control, a short archive with a named member and an explicit structure ---"
${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('t_05233_absent.zip::inner.csv', 'CSV', 'x Int32')" 2>&1 | grep -o -m1 -E 'FILE_DOESNT_EXIST|STD_EXCEPTION'

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB_FS}"
rm -rf "${db_dir}"
