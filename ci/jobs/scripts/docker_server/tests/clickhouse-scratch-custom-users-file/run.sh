#!/bin/bash
# Verify that the requested password applies when the configuration selects another users file
# (`user_directories.users_xml.path`), in a configuration file or in the server arguments after `--`: the server
# merges `<users file>.d` into it, not `users.d`, so `clickhouse docker-init` has to write the generated user there.
# Otherwise the shipped `default` user stays open to the network without a password. `entrypoint.sh` of the Ubuntu
# image always writes to `users.d`, so the test runs only on the `FROM scratch` image.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"
password='custom_users_file_password'

# The custom users file is the image's own `users.xml`, where `default` has no password and accepts any address.
config="$(mktemp -d)"
config_cid="$(docker create "$image")"
cid=
trap '[ $? -eq 0 ] || [ -z "$cid" ] || dumpServerLogs "$cid"; docker rm -vf "$config_cid" $cid > /dev/null; rm -rf "$config"' EXIT
docker cp "$config_cid:/etc/clickhouse-server/users.xml" "$config/custom-users.xml"
cat > "$config/custom-users-path.xml" <<'XML'
<clickhouse>
    <user_directories>
        <users_xml>
            <path>custom-users.xml</path>
        </users_xml>
    </user_directories>
</clickhouse>
XML
chmod -R a+rX "$config"

remoteQuery() {
  docker run --rm --link "$cid":clickhouse "$image" \
    clickhouse-client --host clickhouse "$@"
}

# Without arguments: `retry.sh` parses its command line with `getopt`, so it would take `--password` as its own option.
remoteQueryWithPassword() {
  remoteQuery --password "$password" --query 'SELECT 1'
}

# Start the server with the docker arguments before the image ($1) and the server arguments after it (the rest).
checkPassword() {
  local docker_args="$1"
  shift
  # shellcheck disable=SC2086 # `docker_args` is a list of options.
  cid="$(
    docker run -d \
      -e CLICKHOUSE_PASSWORD="$password" \
      -v "$config/custom-users.xml":/etc/clickhouse-server/custom-users.xml:ro \
      $docker_args \
      --name "$(cname)" \
      "$image" "$@"
  )"

  # shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
  . "$TESTS_LIB_DIR/retry.sh" \
    --tries "$CLICKHOUSE_TEST_TRIES" \
    --sleep "$CLICKHOUSE_TEST_SLEEP" \
    remoteQueryWithPassword

  if remoteQuery --query 'SELECT 1' > /dev/null 2>&1; then
    echo "the server accepts a remote connection of user 'default' without the requested password" >&2
    exit 1
  fi

  docker rm -vf "$cid" > /dev/null
  cid=
}

# Selected in a configuration file.
checkPassword "-v $config/custom-users-path.xml:/etc/clickhouse-server/config.d/custom-users-path.xml:ro"

# Selected in the server arguments.
checkPassword "" -- --user_directories.users_xml.path=/etc/clickhouse-server/custom-users.xml

# The legacy `users_config` naming the same `users.xml` as the shipped `users_xml` storage: one users file, which the
# server loads once, so the setup applies to it rather than refusing several users files.
cat > "$config/legacy-users-config.xml" <<'XML'
<clickhouse>
    <users_config>users.xml</users_config>
</clickhouse>
XML
chmod a+r "$config/legacy-users-config.xml"
checkPassword "-v $config/legacy-users-config.xml:/etc/clickhouse-server/config.d/legacy-users-config.xml:ro"

# A users file path through a symbolic link followed by `..`: `/mounted/link/../users.xml` is `/mounted/sub/users.xml`
# for the server, which lists `/mounted/sub/users.d`, while its lexically normal form is `/mounted/users.xml`. The server
# opens the files it lists there through the lexically normal path, so it cannot load a generated user from either
# directory: writing to one of them would start it without the password, or not at all. The container must refuse it.
volume="$(docker volume create)"
trap '[ $? -eq 0 ] || [ -z "$cid" ] || dumpServerLogs "$cid"; docker rm -vf "$config_cid" $cid > /dev/null; docker volume rm -f "$volume" > /dev/null; rm -rf "$config"' EXIT
docker run --rm \
  -v "$volume":/mounted \
  -v "$config/custom-users.xml":/src/users.xml:ro \
  busybox:musl \
  sh -c 'mkdir -p /mounted/sub/dir && cp /src/users.xml /mounted/users.xml && cp /src/users.xml /mounted/sub/users.xml && ln -s sub/dir /mounted/link && chmod -R a+rX /mounted'
cat > "$config/symlink-users-path.xml" <<'XML'
<clickhouse>
    <user_directories>
        <users_xml>
            <path>/mounted/link/../users.xml</path>
        </users_xml>
    </user_directories>
</clickhouse>
XML
chmod a+r "$config/symlink-users-path.xml"

# Start with the docker arguments ($1) and check that the container refuses the user setup with a message
# containing $2, before any server accepts connections.
checkRefused() {
  local docker_args="$1" message="$2"
  # shellcheck disable=SC2086 # `docker_args` is a list of options.
  cid="$(
    docker run -d \
      -e CLICKHOUSE_PASSWORD="$password" \
      $docker_args \
      --name "$(cname)" \
      "$image"
  )"
  # A server that started anyway keeps running, and `docker wait` does not return.
  if ! exit_code="$(timeout 120 docker wait "$cid")"; then
    echo "the container is still running instead of refusing the user setup ($message)" >&2
    exit 1
  fi
  if [ "$exit_code" = 0 ]; then
    echo "the container exited with code 0 instead of refusing the user setup ($message)" >&2
    exit 1
  fi
  if ! docker logs "$cid" 2>&1 | grep -F "$message" > /dev/null; then
    echo "the container failed for another reason than: $message" >&2
    exit 1
  fi
  docker rm -vf "$cid" > /dev/null
  cid=
}

checkRefused "-v $volume:/mounted -v $config/symlink-users-path.xml:/etc/clickhouse-server/config.d/symlink-users-path.xml:ro" \
  'after a symbolic link'

# A users file path that refers to a property from ZooKeeper: the server expands `${macros.users_file}` only after it
# resolves the ZooKeeper includes, so the file is not known to `clickhouse docker-init`, and writing the generated user
# next to the default would leave the file from ZooKeeper without the password. No ZooKeeper is needed: the container
# must refuse it before starting any server.
cat > "$config/zk-users-path.xml" <<'XML'
<clickhouse>
    <macros>
        <users_file from_zk="/users_file"/>
    </macros>
    <user_directories>
        <users_xml>
            <path>${macros.users_file}</path>
        </users_xml>
    </user_directories>
</clickhouse>
XML
chmod a+r "$config/zk-users-path.xml"
checkRefused "-v $config/zk-users-path.xml:/etc/clickhouse-server/config.d/zk-users-path.xml:ro" \
  'refers to other properties'
