#!/usr/bin/env bash

# A presigned URL carries its credential in a query parameter, and that parameter's value ends at the
# next `&`, at `#`, or at the end of the text. When such a URL is the last thing in a SQL literal, the
# masking has to know where the literal ends: masking the already-quoted text replaces the closing
# quote along with the credential, e.g. `s3_base = 'https://b/f.csv?X-Amz-Signature=[HIDDEN]`, and
# `SHOW CREATE SETTINGS PROFILE` then returns something that is no longer parseable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROFILE_NAME="p05136_$CLICKHOUSE_DATABASE"
CANARY="c05136presignedsignature"

check_profile()
{
    local setting_assignment="$1"
    local create_query

    $CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $PROFILE_NAME"
    $CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE $PROFILE_NAME SETTINGS $setting_assignment"
    create_query=$($CLICKHOUSE_CLIENT --format TSVRaw -q "SHOW CREATE SETTINGS PROFILE $PROFILE_NAME")

    echo "$create_query" | grep -c -F '[HIDDEN]'
    echo "$create_query" | grep -c -F "$CANARY"
    if echo "$create_query" | $CLICKHOUSE_FORMAT --oneline > /dev/null 2>&1; then
        echo "parses"
    else
        echo "does not parse"
    fi
}

# 1. The credential is the last query parameter, so it has no `&` to end it.
check_profile "s3_base = 'https://bucket.s3.amazonaws.com/f.csv?X-Amz-Credential=AKIAIOSFODNN7EXAMPLE&X-Amz-Signature=$CANARY'"

# 2. The credential is followed by a parameter that is not masked, which ends it before the quote.
check_profile "s3_base = 'https://bucket.s3.amazonaws.com/f.csv?X-Amz-Signature=$CANARY&response-content-type=text/csv'"

# 3. The credential is in the userinfo instead, which ends at `@` and never at the closing quote.
check_profile "format_avro_schema_registry_url = 'http://user:$CANARY@registry:8080/'"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE $PROFILE_NAME"
