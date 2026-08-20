#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs USE_AZURE_BLOB_STORAGE.
#
# Regression test for STID 2508-34fb (`stoi` sub-variant): server abort with
# "Logical error: 'std::exception. Code: 1001, type: std::invalid_argument,
# e.what() = stoi: no conversion'" when an Azure connection string contains a
# `BlobEndpoint` URL whose port substring is empty or non-numeric.
#
# Root cause: `contrib/azure/sdk/core/azure-core/src/http/url.cpp:49` calls
# `std::stoi` on the port substring without validating it is non-empty.
# ClickHouse then re-throws this `std::invalid_argument` up to
# `executeQueryImpl`, where `getCurrentExceptionMessageAndPattern` catches
# `std::logic_error` and calls `abortOnFailedAssertion` in debug/sanitizer
# builds (`Common/Exception.cpp:522`). The same abort path fires in release
# builds when `abort_on_logical_error` is set (it is set in
# `tests/config/config.d/abort_on_logical_error.yaml` for the test server).
#
# Fix: `AzureBlobStorageCommon.cpp` translates `std::logic_error` subtypes
# raised by the Azure SDK into a `DB::Exception` with `BAD_ARGUMENTS`.
#
# Why this is a `.sh` test using `clickhouse-local`, not a `.sql` test:
# Without the fix, the test query causes the server to abort. The functional
# test runner connects to the test server; when the server dies it cannot
# observe the test's exception code and the bugfix-validation job sees only
# "test runner was terminated unexpectedly" instead of an `XFAIL` test result.
# `clickhouse-local` is process-isolated, so the abort (or, in release builds,
# the propagated `std::logic_error`) only kills the local process and the
# test runner stays alive. With the fix, `clickhouse-local` returns a clean
# `BAD_ARGUMENTS` error whose message contains the literal `BAD_ARGUMENTS`,
# which the `grep` below picks up.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Reusable connection-string prefix (Azurite default account key, public).
CONN_PREFIX="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

# 1. Non-numeric port in BlobEndpoint -> Azure SDK std::stoi("abc") -> std::invalid_argument
${CLICKHOUSE_LOCAL} --query "SELECT * FROM azureBlobStorage(
    '${CONN_PREFIX};BlobEndpoint=http://devstoreaccount1.blob.core.windows.net:abc/;',
    'cont',
    'p',
    'CSV')
SETTINGS max_threads = 1" 2>&1 | grep -oE 'BAD_ARGUMENTS' | head -n 1

# 2. Empty port substring (':' immediately followed by '/') -> std::stoi("") -> std::invalid_argument
${CLICKHOUSE_LOCAL} --query "SELECT * FROM azureBlobStorage(
    '${CONN_PREFIX};BlobEndpoint=http://devstoreaccount1.blob.core.windows.net:/;',
    'cont',
    'p',
    'CSV')
SETTINGS max_threads = 1" 2>&1 | grep -oE 'BAD_ARGUMENTS' | head -n 1

# 3. Port number overflow (> uint16 max) -> Azure SDK std::out_of_range
${CLICKHOUSE_LOCAL} --query "SELECT * FROM azureBlobStorage(
    '${CONN_PREFIX};BlobEndpoint=http://devstoreaccount1.blob.core.windows.net:99999999999999999999/;',
    'cont',
    'p',
    'CSV')
SETTINGS max_threads = 1" 2>&1 | grep -oE 'BAD_ARGUMENTS' | head -n 1

echo 'ok'
