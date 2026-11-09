#!/bin/bash
# Verify that a non-executable `*.sh` init script stops at its first failed command, like `set -e` in
# `entrypoint.sh`, which sources it: otherwise the rest of the script runs, the server starts after an
# incomplete initialization, and the next starts skip it because the data directory is not empty.
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

initdb="$(mktemp -d)"
shell_dir=""
busybox_cid=""
chmod 755 "$initdb"
cat > "$initdb/init.sh" <<'SCRIPT'
clickhouse client --query 'SELECT throwIf(1)'
echo 'init-script-continued-after-failure'
SCRIPT
chmod 644 "$initdb/init.sh"

# The default image has no shell: mount a statically linked busybox as `/bin/sh`, as its documentation suggests.
shell_args=()
if ! docker run --rm --entrypoint /bin/sh "$image" -c true > /dev/null 2>&1; then
  shell_dir="$(mktemp -d)"
  busybox_cid="$(docker create busybox:musl)"
  docker cp "$busybox_cid:/bin/busybox" "$shell_dir/sh"
  docker rm -f "$busybox_cid" > /dev/null
  busybox_cid=""
  chmod 755 "$shell_dir" "$shell_dir/sh"
  shell_args=(-v "$shell_dir/sh":/bin/sh:ro)
fi

cid="$(
  docker run -d \
    "${shell_args[@]}" \
    -v "$initdb":/docker-entrypoint-initdb.d:ro \
    --name "$(cname)" \
    "$image"
)"
trap 'docker rm -vf "$cid" > /dev/null; rm -rf "$initdb" ${shell_dir:+"$shell_dir"}' EXIT

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
if docker logs "$cid" 2>&1 | grep -F 'init-script-continued-after-failure' > /dev/null; then
  echo "the init script continued after its failed command" >&2
  dumpServerLogs "$cid"
  exit 1
fi
