#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs s3

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
INSERT INTO TABLE FUNCTION s3('http://localhost:11111/test/content-type.csv', 'test', 'testtest', 'CSV', 'number UInt64') SELECT number FROM numbers(1000000) SETTINGS s3_max_single_part_upload_size = 10000, s3_truncate_on_insert = 1;
"

aws --endpoint-url http://localhost:11111 s3api head-object --bucket test --key content-type.csv | grep Content | sed 's/[ \t,"]*//g'
