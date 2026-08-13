#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A serialization version assigned for one connection must not stick to the column type of the table.
# The type object is shared: it lives in the table's column description, so pinning version 0 for a
# client that predates versioning would leave every later query reading the state at that version.
# `sumMap` over `Decimal32` stores 4-byte values at version 0 and 16-byte ones at version 1, so the
# values read back change, not just the printed version.

# `client_protocol_version` below the revision that introduced versioning makes the response ask for
# version 0. The type name the response carries is printed as the oracle that it really did: a
# request that failed, or one that kept version 1, would leave the type untouched and every
# assertion after it would hold trivially. `Native` writes that name uncompressed after the column
# name, and version 0 is the one version that is not printed in it.

STATE="sumMapState(CAST([1, 2], 'Array(UInt64)'), CAST([10.5, 20.25], 'Array(Decimal32(2))'))"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_agg_version_sticky;

    CREATE TABLE t_agg_version_sticky (k UInt32, s AggregateFunction(sumMap, Array(UInt64), Array(Decimal32(2))))
    ENGINE = MergeTree ORDER BY k;

    INSERT INTO t_agg_version_sticky SELECT 1, $STATE;

    SELECT toTypeName(s), finalizeAggregation(s) FROM t_agg_version_sticky;"

${CLICKHOUSE_CURL} -sS --fail "${CLICKHOUSE_URL}&client_protocol_version=54451&query=SELECT+s+FROM+t_agg_version_sticky+FORMAT+Native" \
    | grep -c -a 'AggregateFunction(sumMap, Array(UInt64), Array(Decimal(9, 2)))'

$CLICKHOUSE_CLIENT --query "
    SELECT toTypeName(s), finalizeAggregation(s) FROM t_agg_version_sticky;

    SELECT type FROM system.columns
    WHERE database = currentDatabase() AND table = 't_agg_version_sticky' AND name = 's';

    DROP TABLE t_agg_version_sticky;"

# A Variant addresses its elements by name: the discriminator order and the serialization a reader
# picks both come from the names alone. Assigning a version rewrites a name, since version 0 is
# omitted from it while a default version is printed, so versions are not assigned inside a Variant.
# The bytes a Variant column is served as therefore do not depend on this assignment.

# The state below is stored under the first element, whose version-less spelling is the spelling of
# the second, and served to a client that asks for version 0. Reading the state back is the oracle:
# an element rebuilt under the other name would fail to be addressed at all, and one rebuilt with a
# version 0 name would have its version-1 payload read as a version-0 one.

VARIANT="Variant(AggregateFunction(0, sumMap, Array(UInt32), Array(UInt32)), AggregateFunction(1, sumMap, Array(UInt32), Array(UInt32)))"
NATIVE="$CLICKHOUSE_TMP/04613_variant.native"

$CLICKHOUSE_LOCAL --query "
    SET allow_experimental_variant_type = 1, allow_suspicious_variant_types = 1;

    SELECT CAST(CAST(sumMapState(CAST([1, 2], 'Array(UInt32)'), CAST([10, 20], 'Array(UInt32)')),
                     'AggregateFunction(1, sumMap, Array(UInt32), Array(UInt32))'), '$VARIANT') AS v
    INTO OUTFILE '$NATIVE' TRUNCATE FORMAT Native;"

$CLICKHOUSE_LOCAL --query "
    SET allow_experimental_variant_type = 1, allow_suspicious_variant_types = 1;

    SELECT variantType(v), finalizeAggregation(variantElement(v, 'AggregateFunction(1, sumMap, Array(UInt32), Array(UInt32))'))
    FROM file('$NATIVE', Native);"

rm -f "$NATIVE"
