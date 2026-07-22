#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: requires the ORC input format, which is not built in fasttest.

# Regression test for an out-of-bounds read in the native ORC reader on a malformed file.
#
# Schema inference peeks the first stripe to decide whether a string column is dictionary encoded
# (setting `input_format_orc_dictionary_as_low_cardinality`, on by default). That calls
# `orc::StripeInformation::getColumnEncoding(colId)`, which indexes the stripe footer's repeated
# `ColumnEncoding` field. A malformed file can declare a column id (via the schema) that is not
# present in the stripe footer, so the access ran out of bounds: an abort in debug builds (via the
# protobuf bounds assertion) and undefined behavior in release builds.
#
# The data file's schema declares a single string column `s`, but its stripe footer has fewer
# column encodings, so `getColumnEncoding` for `s` used to read past the end. The reader must now
# reject the file cleanly instead of crashing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/data_orc/orc_stripe_footer_short_encodings.orc

# Must fail cleanly with a schema-inference error (and not abort the server).
$CLICKHOUSE_LOCAL --query "DESC file('$DATA_FILE', ORC) SETTINGS input_format_orc_dictionary_as_low_cardinality = 1" 2>&1 \
    | grep -oF 'out of range' | head -n1
