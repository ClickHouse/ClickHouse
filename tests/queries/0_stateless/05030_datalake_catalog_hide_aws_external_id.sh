#!/usr/bin/env bash
# Regression test: aws_external_id is the shared secret of the AWS AssumeRole triple, and
# aws_role_session_name can be one too (a trust policy can require a specific value through the
# sts:RoleSessionName condition), so both must be redacted as [HIDDEN] when a DataLakeCatalog CREATE
# query is formatted (system.databases.engine_full, SHOW CREATE DATABASE), while aws_role_arn is a
# non-secret identifier that stays visible. Uses clickhouse-format so it needs no live catalog and is
# safe to run in parallel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SECRET="SECRET_THAT_MUST_NOT_LEAK"

query="CREATE DATABASE d ENGINE = DataLakeCatalog('http://example.invalid/catalog') SETTINGS
catalog_type = 'glue',
region = 'us-east-1',
aws_access_key_id = '${SECRET}',
aws_secret_access_key = '${SECRET}',
aws_external_id = '${SECRET}',
aws_role_arn = 'arn:aws:iam::1:role/r',
aws_role_session_name = '${SECRET}'"

show_settings() {
    local formatted="$1"
    for setting in \
        aws_external_id \
        aws_access_key_id \
        aws_secret_access_key \
        aws_role_arn \
        aws_role_session_name
    do
        echo "$formatted" | grep -oE "(^|[, ])${setting} = '[^']*'" | sed -E "s/^[, ]//"
    done
}

# Arm A: at the default the secrets are redacted; arms C (the non-secret role ARN stays visible) and
# D (sibling AWS keys still redacted) are asserted by the same output.
echo "--- default: aws_external_id and aws_role_session_name hidden, aws_role_arn visible"
formatted=$(echo "$query" | $CLICKHOUSE_FORMAT --oneline)
show_settings "$formatted"
if echo "$formatted" | grep -q "$SECRET"; then
    echo "FAIL: secret leaked in formatted query"
else
    echo "OK: no secret in formatted query"
fi

# Arm B: an authorized caller can still retrieve the value, so the fix redacts rather than destroys.
echo "--- show_secrets: aws_external_id and aws_role_session_name visible"
formatted=$(echo "$query" | $CLICKHOUSE_FORMAT --oneline --show_secrets)
show_settings "$formatted"
