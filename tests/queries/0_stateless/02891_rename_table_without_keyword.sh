#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db_name=db_$(echo $RANDOM |md5sum |cut -c 1-4)

$CLICKHOUSE_CLIENT --param_db="${db_name}" --multiquery \
    --query="DROP DATABASE IF EXISTS {db:Identifier};
             CREATE DATABASE {db:Identifier};
             CREATE TABLE IF NOT EXISTS {db:Identifier}.r1 (name String) Engine=Memory();
             SHOW TABLES FROM {db:Identifier}"

$CLICKHOUSE_CLIENT --param_db="${db_name}" --multiquery \
    --query="RENAME TABLE {db:Identifier}.r1 TO {db:Identifier}.r1_bak;
             SHOW TABLES FROM {db:Identifier};"

$CLICKHOUSE_CLIENT --param_db="${db_name}" --multiquery \
    --query="RENAME {db:Identifier}.r1_bak TO {db:Identifier}.r1;
             SHOW TABLES FROM {db:Identifier};"

$CLICKHOUSE_CLIENT --param_db="${db_name}" --multiquery \
    --query="CREATE TABLE IF NOT EXISTS {db:Identifier}.r2 (name String) Engine=Memory();
             RENAME {db:Identifier}.r1 TO {db:Identifier}.r1_bak, {db:Identifier}.r2 TO {db:Identifier}.r2_bak;
             SHOW TABLES FROM {db:Identifier};"


$CLICKHOUSE_CLIENT --param_db="${db_name}" --multiquery \
    --query="CREATE TABLE IF NOT EXISTS {db:Identifier}.source_table (
                id UInt64,
                value String
            ) ENGINE = Memory;
            
            CREATE DICTIONARY IF NOT EXISTS {db:Identifier}.test_dictionary
            (
                id UInt64,
                value String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE '{db:Identifier}.source_table'))
            LAYOUT(FLAT())
            LIFETIME(MIN 0 MAX 1000);
            
            SHOW DICTIONARIES FROM {db:Identifier};"

 
$CLICKHOUSE_CLIENT --param_db="${db_name}" --multiquery \
    --query="RENAME {db:Identifier}.test_dictionary TO {db:Identifier}.test_dictionary_2;
             SHOW DICTIONARIES FROM {db:Identifier};"

todb_name=db_$(echo $RANDOM |md5sum |cut -c 1-4)

$CLICKHOUSE_CLIENT --param_todb="${todb_name}" --param_db="${db_name}" --query="RENAME {db:Identifier} TO {todb:Identifier}; -- { serverError 60 }" 2>&1  | grep -o "UNKNOWN_TABLE" | uniq

$CLICKHOUSE_CLIENT --param_db="${db_name}" --query="DROP DATABASE IF EXISTS {db:Identifier}"

