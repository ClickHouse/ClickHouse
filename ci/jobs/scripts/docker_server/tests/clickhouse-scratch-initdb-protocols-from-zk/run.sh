#!/bin/bash
# Verify that the container does not start the initialization when the configuration includes elements from ZooKeeper
# (`from_zk`): `clickhouse docker-init` binds every protocol of the temporary server to localhost, but the protocols
# from ZooKeeper are known only to the server, which could then accept remote connections before the initialization
# (e.g. setting the password) is done. `entrypoint.sh` of the Ubuntu image does not check that, so the test runs only
# on the `FROM scratch` image.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

cid="$(
  docker run -d \
    -v "$dir/initdb.sql":/docker-entrypoint-initdb.d/initdb.sql:ro \
    -v "$dir/protocols-from-zk.xml":/etc/clickhouse-server/config.d/protocols-from-zk.xml:ro \
    --name "$(cname)" \
    "$image"
)"
trap 'docker rm -vf "$cid" > /dev/null' EXIT

# A server that started anyway keeps running, and `docker wait` does not return.
if ! exit_code="$(timeout 120 docker wait "$cid")"; then
  echo "the container is still running instead of failing" >&2
  dumpServerLogs "$cid"
  exit 1
fi
if [ "$exit_code" = 0 ]; then
  echo "the container exited with code 0 instead of failing" >&2
  dumpServerLogs "$cid"
  exit 1
fi
if ! docker logs "$cid" 2>&1 | grep -F 'from_zk' > /dev/null; then
  echo "the container failed for another reason than the elements included from ZooKeeper" >&2
  dumpServerLogs "$cid"
  exit 1
fi
