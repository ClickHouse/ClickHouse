#!/usr/bin/env bash
# `EXISTS DICTIONARY` (and a plain `EXISTS` asked by a caller who only holds `SHOW DICTIONARIES`)
# proves the source-side grant from metadata alone and then has to load the source object to tell a
# dictionary from a table. Behind a read-only `Overlay` facade that load must not surface the source
# object's own error: `SHOW DICTIONARIES` says nothing about a source object that is not a
# dictionary, so its error would distinguish a hidden object from a name that does not exist.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
# The file exists, so the name resolves through a `Filesystem` source from metadata alone, but the
# format cannot be detected from its extension, so actually opening it throws that source's error.
printf 'not a table\n' > "${DIR}"/broken.unknownformat

DB_FS="db_fs_${CLICKHOUSE_DATABASE}"
DB_SRC="db_src_${CLICKHOUSE_DATABASE}"
DB_OVL="db_ovl_${CLICKHOUSE_DATABASE}"
USER_DICT="u_dict_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_FS};
DROP DATABASE IF EXISTS ${DB_SRC};
DROP USER IF EXISTS ${USER_DICT};

CREATE DATABASE ${DB_FS} ENGINE = Filesystem;
CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
CREATE TABLE ${DB_SRC}.numbers_source (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE DICTIONARY ${DB_SRC}.dict (id UInt64, value String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(DB '${DB_SRC}' TABLE 'numbers_source'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0);
CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}', '${DB_FS}');

CREATE USER ${USER_DICT} NOT IDENTIFIED;
-- A dictionary-only caller: no SHOW TABLES anywhere.
GRANT SHOW DICTIONARIES ON ${DB_OVL}.* TO ${USER_DICT};
GRANT SHOW DICTIONARIES ON ${DB_SRC}.* TO ${USER_DICT};
GRANT SHOW DICTIONARIES ON ${DB_FS}.* TO ${USER_DICT};
"

BROKEN="${CLICKHOUSE_TEST_UNIQUE_NAME}/broken.unknownformat"
ABSENT="${CLICKHOUSE_TEST_UNIQUE_NAME}/absent.unknownformat"

answer()
{
    $CLICKHOUSE_CLIENT --user "${USER_DICT}" -q "$1" 2>&1 \
        | grep -oE '^[01]$|CANNOT_DETECT_FORMAT|Code: [0-9]+' | sort -u | tr '\n' ' '
    echo
}

echo 'a source object that is not a dictionary answers like a missing name'
answer "EXISTS DICTIONARY ${DB_OVL}.\`${BROKEN}\`"
answer "EXISTS DICTIONARY ${DB_OVL}.\`${ABSENT}\`"
answer "EXISTS ${DB_OVL}.\`${BROKEN}\`"
answer "EXISTS ${DB_OVL}.\`${ABSENT}\`"

echo 'a real dictionary of a source is still visible through the facade'
answer "EXISTS DICTIONARY ${DB_OVL}.dict"
answer "EXISTS ${DB_OVL}.dict"

$CLICKHOUSE_CLIENT -m -q "
DROP USER ${USER_DICT};
DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_FS};
DROP DATABASE ${DB_SRC};
"

rm -rf "${DIR}"
