#!/usr/bin/env bash
# Tags: fasttest-only
# Tag fasttest-only - this test requires lexer_test which is only available
# in fast-test environment because it is built along with clickhouse but is not
# transfered as an artifact to other test environments


CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

lexer_test 'SELECT 1, 2, /* Hello */ "test" AS x'
