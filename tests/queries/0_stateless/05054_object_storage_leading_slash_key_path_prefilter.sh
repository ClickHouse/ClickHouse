#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires Azurite

# A `_path` / `_file` prefilter must format its candidate paths exactly the way the `_path` column
# is produced, which is `joinPathUnderPrefix(namespace, key)`: it drops one leading separator from
# the key so that the namespace survives the join. Plain `fs::path(namespace) / key` does not - an
# absolute right-hand side replaces the left-hand side - so a key that keeps a leading separator
# loses the namespace and no candidate ever matches the column, pruning away the only object.
#
# Azure is the reachable backend for such a key: `S3::URI::validateKey` rejects it. The arms below
# cover the two prefilters that see materialized keys rather than listed objects, and each compares
# the two spellings against each other rather than against a literal:
#   - the brace-expansion branch of `createFileIterator`, which builds candidates for the expanded
#     paths before `KeysIterator` is constructed;
#   - the deferred filter in `KeysIterator::next`, reached when a set is not ready while the plan is
#     built, which is what `GLOBAL IN` gives.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# A server warning would land on stderr, which clickhouse-test turns into a failure.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL="none"
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AZURE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
# Azure container names are limited to 63 characters and must be lowercase alphanumeric, so hash
# the unique name instead of embedding it.
AZURE_CONT="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"

ONE="'${AZURE_CONN}', '${AZURE_CONT}', '/slashed1.csv', 'CSV', 'auto', 'x UInt64'"
TWO="'${AZURE_CONN}', '${AZURE_CONT}', '/slashed2.csv', 'CSV', 'auto', 'x UInt64'"
BRACE="'${AZURE_CONN}', '${AZURE_CONT}', '/{slashed1,slashed2}.csv', 'CSV', 'auto', 'x UInt64'"

${CLICKHOUSE_CLIENT} -q "
INSERT INTO FUNCTION azureBlobStorage(${ONE}) SELECT 1 AS x SETTINGS azure_truncate_on_insert = 1;
INSERT INTO FUNCTION azureBlobStorage(${TWO}) SELECT 2 AS x SETTINGS azure_truncate_on_insert = 1;
" > /dev/null

# The one key is materialized, so it is pruned by the non-glob prefilter rather than while listing.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage(${ONE}) WHERE _path IN (SELECT _path FROM azureBlobStorage(${ONE}));"

# `GLOBAL IN` leaves the set unbuilt while the plan is optimized, so the filter is deferred to
# `KeysIterator::next` and formats the path a second time there.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage(${ONE}) WHERE _path GLOBAL IN (SELECT _path FROM azureBlobStorage(${ONE}));"

# A brace expansion is materialized locally too, and takes its own prefilter branch.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage(${BRACE}) WHERE _path IN (SELECT _path FROM azureBlobStorage(${BRACE}));"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage(${BRACE}) WHERE _path GLOBAL IN (SELECT _path FROM azureBlobStorage(${BRACE}));"

# The pruning itself must keep working: a predicate that matches nothing still reads nothing.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage(${BRACE}) WHERE _path GLOBAL IN (SELECT 'no such path');"

# `_file` is derived from the same string, so it must not lose the key either.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage(${BRACE}) WHERE _file GLOBAL IN (SELECT _file FROM azureBlobStorage(${BRACE}));"

# A general glob takes the third prefilter: `_path` values extracted from the filter replace the
# listing. Inverting the `_path` formatting is ambiguous there - `container/slashed1.csv` is what
# both `/slashed1.csv` and `slashed1.csv` render to - so the glob decides: it accepts only the
# spelling with the separator here, and that is the object that must be read.
GLOB_SLASHED="'${AZURE_CONN}', '${AZURE_CONT}', '/*.csv', 'CSV', 'auto', 'x UInt64'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage(${GLOB_SLASHED}) WHERE _path = '${AZURE_CONT}/slashed1.csv';"

# The extracted keys stand in for the listing, so a value that fits the glob but names no blob must
# behave like a listing that does not contain it: no rows, no exception. A mixed predicate reads the
# existing blob and silently drops the missing one.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage(${GLOB_SLASHED}) WHERE _path = '${AZURE_CONT}/missing.csv';"
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM azureBlobStorage(${GLOB_SLASHED}) WHERE _path IN ('${AZURE_CONT}/slashed1.csv', '${AZURE_CONT}/missing.csv');"

# When the glob accepts both spellings the extraction cannot pick one, so the listing must decide
# instead of a guess reading the wrong (or a missing) blob.
GLOB_ANY="'${AZURE_CONN}', '${AZURE_CONT}', '**.csv', 'CSV', 'auto', 'x UInt64'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM azureBlobStorage(${GLOB_ANY}) WHERE _path = '${AZURE_CONT}/slashed1.csv';"
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM azureBlobStorage(${GLOB_ANY});"
