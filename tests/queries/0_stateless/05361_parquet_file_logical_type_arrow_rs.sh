#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading a file with groups annotated with the Parquet `FILE` logical type written by arrow-rs
# (the `parquet` crate, apache/arrow-rs commit dfef34e67737ee6847ded8b15f49751719375f1b) with the
# low-level column writer API, with the schema:
#
#   message schema {
#       required int64 id;
#       optional group file (FILE) {
#           optional binary uri (STRING);
#           optional int64 offset;
#           optional int64 size;
#           optional binary content_type (STRING);
#           optional binary checksum (STRING);
#           optional binary inline;
#       }
#       required group inline_file (FILE) {
#           optional binary inline;
#           optional binary content_type (STRING);
#       }
#       optional group files (LIST) {
#           repeated group file (FILE) {
#               optional binary uri (STRING);
#           }
#       }
#   }
#
# The third row has a null `file`, the second row sets `size` alongside `inline` without `uri` (an
# invalid locator, which is returned as stored). `files` is a two-level list, whose repeated group is
# the element.

DATA="$CUR_DIR/data_parquet/file_logical_type_arrow_rs.parquet"

$CLICKHOUSE_LOCAL -q "DESCRIBE file('${DATA}')"
$CLICKHOUSE_LOCAL -q "SELECT id, file, hex(inline_file.inline), inline_file.content_type, files FROM file('${DATA}') ORDER BY id"
$CLICKHOUSE_LOCAL -q "SELECT id, file.uri, file.offset, file.size FROM file('${DATA}') WHERE file.size > 10 ORDER BY id"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('${DATA}', Parquet, 'id Int64, file Tuple(inline Nullable(String), uri Nullable(String))') ORDER BY id"
