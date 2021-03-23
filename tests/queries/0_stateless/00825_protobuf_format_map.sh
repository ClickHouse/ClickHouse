#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_map_type = 1;

DROP TABLE IF EXISTS map_protobuf_00825;

CREATE TABLE map_protobuf_00825
(
  a Map(UInt32)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO map_protobuf_00825 VALUES ({'x':5, 'y':7}), ({'z':11}), ({'temp':0}), ({'':0});

SELECT * FROM map_protobuf_00825;
EOF

# FIXME(map): add ColumnMap support in ProtobufSerializer.cpp
