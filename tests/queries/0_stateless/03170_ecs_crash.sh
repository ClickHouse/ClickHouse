#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Previous versions crashed in attempt to use this authentication method (regardless of whether it was able to authenticate):
AWS_CONTAINER_CREDENTIALS_FULL_URI=http://localhost:1338/latest/meta-data/container/security-credentials $CLICKHOUSE_LOCAL -q "select * from s3('http://localhost:11111/test/a.tsv')"
