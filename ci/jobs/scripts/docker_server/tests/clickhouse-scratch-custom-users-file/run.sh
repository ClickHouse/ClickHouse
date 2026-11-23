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
    remoteQuery --password "$password" --query 'SELECT 1'

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
