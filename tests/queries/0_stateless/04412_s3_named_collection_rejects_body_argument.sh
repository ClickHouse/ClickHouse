#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: the s3 table function is unavailable in fasttest builds.
# Tag no-replicated-database: a named collection is a global object.

# The body(...) argument is supported only by the `url` table function, not by S3. The positional
# argument parser rejects it, but the named-collection path used to ignore non-`equals` function
# arguments (with the default allow_named_collection_override_by_default = 1), so
# s3(collection, body(...)) was accepted while the body was silently dropped. It must be rejected
# loudly with BAD_ARGUMENTS instead. The error is thrown during argument parsing, before any
# subquery is evaluated and before any connection to the URL is made.
#
# The named collection name is derived from $CLICKHOUSE_DATABASE so it is unique per test run. A
# named collection is a global server object, so a fixed name would race when the flaky check runs
# the same test concurrently (NAMED_COLLECTION_ALREADY_EXISTS). This is also why the test cannot be
# a plain .sql file: a query parameter is not accepted as a CREATE NAMED COLLECTION name.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

COLL="${CLICKHOUSE_DATABASE}_s3_body"

$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION IF EXISTS ${COLL}"
$CLICKHOUSE_CLIENT --query "CREATE NAMED COLLECTION ${COLL} AS url = 'http://localhost:11111/test/data.csv'"

for analyzer in 1 0; do
    for arg in "body('payload')" "body((SELECT 1))" "body('')"; do
        result=$($CLICKHOUSE_CLIENT --enable_analyzer="${analyzer}" \
            --query "SELECT * FROM s3(${COLL}, ${arg})" 2>&1)
        if echo "${result}" | grep -qF "BAD_ARGUMENTS"; then
            echo "BAD_ARGUMENTS"
        else
            echo "UNEXPECTED (analyzer=${analyzer}, ${arg}): ${result}"
        fi
    done
done

$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION ${COLL}"
