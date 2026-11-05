#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: requires the MySQL engine omitted from the fast build.
# Tag no-replicated-database: named collections are server-global, and `DETACH TABLE` is not allowed in a `Replicated` database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_dict"
nc_locked="${CLICKHOUSE_TEST_UNIQUE_NAME}_locked"
nc_load="${CLICKHOUSE_TEST_UNIQUE_NAME}_load"
nc_mysql="${CLICKHOUSE_TEST_UNIQUE_NAME}_mysql"
nc_url="${CLICKHOUSE_TEST_UNIQUE_NAME}_url"
user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"

function cleanup()
{
    # A client killed inside attach_is_rejected leaves its table detached permanently, which DROP TABLE does not see.
    for table in $(${CLICKHOUSE_CLIENT} --query "SELECT table FROM system.detached_tables
        WHERE database = currentDatabase() AND table IN ('table_settings_override', 'table_alias_override')"); do
        ${CLICKHOUSE_CLIENT} --multiquery --query "
            SET ast_fuzzer_any_query = 0;
            ALTER NAMED COLLECTION IF EXISTS $nc_mysql SET connection_pool_size = 2 OVERRIDABLE;
            ALTER NAMED COLLECTION IF EXISTS $nc_url SET http_method = 'POST' OVERRIDABLE;
            ATTACH TABLE $table;
        "
    done
    # Neither ignore nor fuzz these drops: each object must be gone before the named collection it references.
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        SET ast_fuzzer_any_query = 0, ignore_drop_queries_probability = 0;
        DROP DICTIONARY IF EXISTS dict_override;
        DROP DICTIONARY IF EXISTS dict_add_key;
        DROP DICTIONARY IF EXISTS dict_alias;
        DROP DICTIONARY IF EXISTS dict_locked;
        DROP DICTIONARY IF EXISTS dict_load;
        DROP TABLE IF EXISTS table_settings_override;
        DROP TABLE IF EXISTS table_alias_override;
        DROP TABLE IF EXISTS dict_source_a;
        DROP TABLE IF EXISTS dict_source_b;
        DROP USER IF EXISTS $user;
        DROP NAMED COLLECTION IF EXISTS $nc;
        DROP NAMED COLLECTION IF EXISTS $nc_locked;
        DROP NAMED COLLECTION IF EXISTS $nc_load;
        DROP NAMED COLLECTION IF EXISTS $nc_mysql;
        DROP NAMED COLLECTION IF EXISTS $nc_url;
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE dict_source_a (id UInt64, value UInt64) ENGINE = Memory;
    CREATE TABLE dict_source_b (id UInt64, value UInt64) ENGINE = Memory;
    INSERT INTO dict_source_a VALUES (1, 100);
    INSERT INTO dict_source_b VALUES (1, 200);

    CREATE NAMED COLLECTION $nc AS
        host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}, user = 'default', password = '',
        db = '${CLICKHOUSE_DATABASE}', table = 'dict_source_a';
    CREATE NAMED COLLECTION $nc_locked AS
        host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}, user = 'default', password = '',
        db = '${CLICKHOUSE_DATABASE}', table = 'dict_source_a' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION $nc_load AS
        host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}, user = 'default', password = '',
        db = '${CLICKHOUSE_DATABASE}', table = 'dict_source_a';

    CREATE USER $user;
    GRANT SOURCES ON *.* TO $user;
    GRANT CREATE DICTIONARY, DROP DICTIONARY, dictGet ON ${CLICKHOUSE_DATABASE}.* TO $user;
    GRANT NAMED COLLECTION ON $nc TO $user;
    GRANT NAMED COLLECTION ON $nc_locked TO $user;
"

echo 'Overriding a stored key in a dictionary source requires the secrets privilege'
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE DICTIONARY dict_override (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(NAME $nc TABLE 'dict_source_b')) LAYOUT(FLAT()) LIFETIME(0); -- { serverError ACCESS_DENIED }
    CREATE DICTIONARY dict_alias (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(NAME $nc DATABASE '${CLICKHOUSE_DATABASE}' TABLE 'dict_source_a')) LAYOUT(FLAT()) LIFETIME(0); -- { serverError ACCESS_DENIED }
"

echo 'Adding a missing key needs no secrets privilege'
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE DICTIONARY dict_add_key (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(NAME $nc WHERE 'id = 1')) LAYOUT(FLAT()) LIFETIME(0);
    SELECT dictGet('dict_add_key', 'value', toUInt64(1));
"

echo 'With the secrets privilege the override is allowed'
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON $nc TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE DICTIONARY dict_override (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(NAME $nc TABLE 'dict_source_b')) LAYOUT(FLAT()) LIFETIME(0);
    SELECT dictGet('dict_override', 'value', toUInt64(1));
    CREATE DICTIONARY dict_alias (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(NAME $nc DATABASE '${CLICKHOUSE_DATABASE}' TABLE 'dict_source_b')) LAYOUT(FLAT()) LIFETIME(0);
    SELECT dictGet('dict_alias', 'value', toUInt64(1));
"

echo 'A NOT OVERRIDABLE key cannot be overridden even with the secrets privilege'
# A separate collection: no stored dictionary overrides a key while it is locked, so a restart can load what the test leaves.
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON $nc_locked TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE DICTIONARY dict_locked (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(NAME $nc_locked TABLE 'dict_source_b')) LAYOUT(FLAT()) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
"

echo 'A dictionary created with an override by a privileged user loads in the background'
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE DICTIONARY dict_load (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(NAME $nc_load TABLE 'dict_source_b')) LAYOUT(FLAT()) LIFETIME(0);
    SELECT dictGet('dict_load', 'value', toUInt64(1));
    SYSTEM RELOAD DICTIONARY dict_load;
    SELECT dictGet('dict_load', 'value', toUInt64(1));
    DETACH DICTIONARY dict_load;
    ATTACH DICTIONARY dict_load;
    SELECT dictGet('dict_load', 'value', toUInt64(1));
"

echo 'A stored table whose override became NOT OVERRIDABLE is rejected when it is attached'
# The fuzzer stays off for all DDL of these tables: a __fuzz_N clone would stay attached while its key is locked.
${CLICKHOUSE_CLIENT} --multiquery --query "
    SET ast_fuzzer_any_query = 0;
    CREATE NAMED COLLECTION $nc_mysql AS
        host = '127.0.0.1', port = 1, user = 'user', password = 'secret', database = 'database', table = 'table',
        connection_pool_size = 2;
    CREATE NAMED COLLECTION $nc_url AS
        url = 'http://127.0.0.1:1/data', format = 'TSV', http_method = 'POST';
    CREATE TABLE table_settings_override (value UInt64) ENGINE = MySQL($nc_mysql) SETTINGS connection_pool_size = 1;
    CREATE TABLE table_alias_override (value UInt64) ENGINE = URL($nc_url, method = 'PUT');
"

# A table that overrides a key which is locked afterwards cannot be attached: the lock is checked on every load.
# The table stays detached permanently while its key is locked, so a server restart never loads it.
function attach_is_rejected()
{
    local table=$1
    local collection=$2
    local key=$3
    local value=$4
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        SET ast_fuzzer_any_query = 0;
        DETACH TABLE $table PERMANENTLY;
        ALTER NAMED COLLECTION $collection SET $key = $value NOT OVERRIDABLE;
    "
    if error=$(${CLICKHOUSE_CLIENT} --multiquery --query "SET ast_fuzzer_any_query = 0; ATTACH TABLE $table" 2>&1); then
        echo "Expected the attach of $table to be rejected"
        exit 1
    fi
    echo "$error" | grep -o "Override not allowed for '$key'" | head -1
    echo "$error" | grep -o 'BAD_ARGUMENTS' | head -1
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        SET ast_fuzzer_any_query = 0;
        ALTER NAMED COLLECTION $collection SET $key = $value OVERRIDABLE;
        ATTACH TABLE $table;
    "
}

attach_is_rejected table_settings_override "$nc_mysql" connection_pool_size 2
attach_is_rejected table_alias_override "$nc_url" http_method "'POST'"
