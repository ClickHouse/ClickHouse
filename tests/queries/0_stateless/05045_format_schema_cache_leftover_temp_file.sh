#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A schema given as a string is published into the schema cache through a temporary file. A
# temporary file already sitting at that path belongs to another query storing the same schema, so
# storing must neither fail because of it nor delete it.

WORKDIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORKDIR"
mkdir -p "$WORKDIR"

SCHEMA='@0x844f048b15c12dab;\nstruct M { data @0 :List(List(Int32)); }'
QUERY="DESC format(CapnProto, '')
SETTINGS
    format_schema_source = 'string',
    format_schema = '${SCHEMA}',
    format_schema_message_name = 'M'"

# The cache directory of clickhouse-local is relative to the working directory.
(cd "$WORKDIR" && ${CLICKHOUSE_LOCAL} --logger.console=0 --query "$QUERY" > /dev/null)

CACHED=$(basename "$(ls "$WORKDIR"/__cache__/*.capnp)")

if [ -z "$CACHED" ]; then
    echo "FAIL: the first query stored no schema file to learn the name from"
fi

# Drop the published schema and leave a temporary file of it behind, so the next query has to store
# the schema again while that temporary path is taken.
rm -f "$WORKDIR"/__cache__/*.capnp
echo 'leftover' > "$WORKDIR/__cache__/$CACHED.tmp"

(cd "$WORKDIR" && ${CLICKHOUSE_LOCAL} --logger.console=0 --query "$QUERY")

if [ -f "$WORKDIR/__cache__/$CACHED.tmp" ]; then
    echo 'leftover file kept'
else
    echo 'leftover file removed'
fi

rm -rf "$WORKDIR"
