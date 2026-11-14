#!/bin/bash
# Verify that the image contains the standard Protobuf schemas (`google_protos_path`), so a format schema can
# import e.g. `google/protobuf/wrappers.proto`. Only the `FROM scratch` image guarantees them (`clickhouse-scratch-*`):
# an image installed from a single binary with `clickhouse install` has no such directory.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

schemas="$(mktemp -d)"
chmod 755 "$schemas"
cat > "$schemas/wrapped.proto" <<'SCHEMA'
syntax = "proto3";
import "google/protobuf/wrappers.proto";
message Wrapped
{
  google.protobuf.StringValue value = 1;
}
SCHEMA
chmod 644 "$schemas/wrapped.proto"

cid="$(
  docker run -d \
    -v "$schemas":/schemas:ro \
    --name "$(cname)" \
    "$image" \
    -- --format_schema_path=/schemas/
)"
trap '[ $? -eq 0 ] || dumpServerLogs "$cid"; docker rm -vf "$cid" > /dev/null; rm -rf "$schemas"' EXIT

# From inside the container, because the `default` user is restricted to localhost without a password.
chCli() {
  docker exec "$cid" clickhouse-client --query "$*"
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  chCli SELECT 1

# The server writes the file, so the schema is resolved with its `google_protos_path`, not the client's.
chCli "INSERT INTO FUNCTION file('wrapped.bin', Protobuf, 'value Nullable(String)') SELECT 'abc' \
  SETTINGS format_schema = 'wrapped:Wrapped', output_format_protobuf_nullables_with_google_wrappers = 1"
[ "$(chCli "SELECT length(raw) > 0 FROM file('wrapped.bin', RawBLOB, 'raw String')")" = 1 ]
