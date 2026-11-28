#!/bin/bash
# Verify that the container does not start when the configuration has several users configuration files and a user
# setup is requested: the server authenticates against every one of them, so the generated configuration, which removes
# or restricts `default` in one file only, would leave the `default` of another file without the password and open to
# the network. `entrypoint.sh` of the Ubuntu image does not check that, so the test runs only on the `FROM scratch` image.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

# The second users file is the image's own `users.xml`, where `default` has no password and accepts any address.
# A storage of another element name (`users_config`) is added next to the shipped `users_xml`, not merged into it.
config="$(mktemp -d)"
config_cid="$(docker create "$image")"
cid=
trap '[ -z "$cid" ] || docker rm -vf "$cid" > /dev/null; docker rm -vf "$config_cid" > /dev/null; rm -rf "$config"' EXIT
docker cp "$config_cid:/etc/clickhouse-server/users.xml" "$config/second-users.xml"
cat > "$config/second-users-storage.xml" <<'XML'
<clickhouse>
    <user_directories>
        <users_config>
            <path>second-users.xml</path>
        </users_config>
    </user_directories>
</clickhouse>
XML
chmod -R a+rX "$config"

cid="$(
  docker run -d \
    -e CLICKHOUSE_USER=service \
    -e CLICKHOUSE_PASSWORD=multiple_users_files_password \
    -v "$config/second-users.xml":/etc/clickhouse-server/second-users.xml:ro \
    -v "$config/second-users-storage.xml":/etc/clickhouse-server/config.d/second-users-storage.xml:ro \
    --name "$(cname)" \
    "$image"
)"

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
if ! docker logs "$cid" 2>&1 | grep -F 'several users configuration files' > /dev/null; then
  echo "the container failed for another reason than the several users configuration files" >&2
  dumpServerLogs "$cid"
  exit 1
fi
