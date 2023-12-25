#!/usr/bin/env bash
# Tags: no-parallel, no-s3-storage, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

clickhouse-client -udefault -mn --query="DROP TABLE IF EXISTS test_sync_server_settings;
                                         CREATE TABLE test_sync_server_settings (id UInt32, dt DateTime) Engine=MergeTree() ORDER BY id;
                                         DROP USER IF EXISTS date_format_writter;
                                         DROP USER IF EXISTS date_writter;
                                         CREATE USER date_format_writter IDENTIFIED WITH plaintext_password BY '123' SETTINGS date_time_input_format = 'best_effort';
                                         CREATE USER date_writter IDENTIFIED WITH plaintext_password BY '123';
                                         GRANT ALL ON test_sync_server_settings TO date_format_writter;
                                         GRANT ALL ON test_sync_server_settings TO date_writter;"

clickhouse-client -udate_writter --password 123 --query="INSERT INTO test_sync_server_settings VALUES (1, '20230101120107')"
clickhouse-client -udate_writter --password 123 --query="SELECT id, toYYYYMMDDhhmmss(dt) = 20230101120107 FROM test_sync_server_settings where id = 1"
clickhouse-client -udate_format_writter --password 123 --query="INSERT INTO test_sync_server_settings VALUES (2, '20230101120107');"
clickhouse-client -udefault --query="SELECT id, toYYYYMMDDhhmmss(dt) = 20230101120107  FROM test_sync_server_settings where id = 2"
clickhouse-client -udefault --query="DROP TABLE IF EXISTS test_sync_server_settings;"

