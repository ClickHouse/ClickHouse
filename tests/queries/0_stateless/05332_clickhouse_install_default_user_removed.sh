#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse install` must not set up a password for the default user when it is removed from the users config.

PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$PREFIX/fakebin" "$PREFIX/usr/bin" "$PREFIX/etc/clickhouse-server/users.d"

# Make `clickhouse install` skip setting capabilities on the real binary.
printf '#!/bin/sh\nexit 1\n' > "$PREFIX/fakebin/capsh"
chmod +x "$PREFIX/fakebin/capsh"

# With `--link` and an existing symlink to the binary, the binary itself is not touched.
BINARY=$(realpath "$(command -v "$CLICKHOUSE_BINARY")")
ln -s "$BINARY" "$PREFIX/usr/bin/clickhouse"

echo '<clickhouse><users><default remove="1"/></users></clickhouse>' > "$PREFIX/etc/clickhouse-server/users.d/remove-default.xml"

function install()
{
    PATH="$PREFIX/fakebin:$PATH" "$BINARY" install --link --prefix "$PREFIX" --user '' --group '' -y < /dev/null 2>&1 \
        | grep -o -e 'The default user is removed' -e 'The default user may be defined in [^ ]*' -e 'Password for the default user is an empty string' \
        -e 'successfully installed'
    ls "$PREFIX/etc/clickhouse-server/users.d"
}

echo "removed"
install

# A stray `.sql` file and an unparsable `<uuid>.sql` file in the local directory storage are not entities.
ACCESS="$PREFIX/var/lib/clickhouse/access"
mkdir -p "$ACCESS"
touch "$ACCESS/need_rebuild_lists.mark"
echo 'ATTACH USER default;' > "$ACCESS/notes.sql"
echo 'garbage' > "$ACCESS/00000000-0000-0000-0000-000000000001.sql"
echo "stray files"
install

# The default user in the local directory storage, which follows the XML users config.
echo 'ATTACH USER default;' > "$ACCESS/00000000-0000-0000-0000-000000000002.sql"
echo "local directory"
install

# The installer makes the config directory read-only.
chmod -R u+w "$PREFIX"
rm -rf "$PREFIX"
