#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Some time zone databases keep alternate copies of the zones in `posix/` (plain copies) and
# `right/` (the same zones counting leap seconds). A host can point its local time zone at one of
# those copies, and then the name ClickHouse derives for it is `posix/Europe/Berlin` rather than
# `Europe/Berlin`. Neither prefixed name is in the time zone database linked into the binary, so
# the prefix has to be stripped instead of making the server refuse to start.
#
# The zone is loaded from the built-in database once its name is known, so what the files in the
# private database below contain does not matter - only where they sit.

TZDIR_LOCAL="${CLICKHOUSE_TMP}/zoneinfo_05233"
rm -rf "${TZDIR_LOCAL}"
mkdir -p "${TZDIR_LOCAL}/Europe" "${TZDIR_LOCAL}/posix/Europe" "${TZDIR_LOCAL}/right/Europe"
touch "${TZDIR_LOCAL}/Europe/Berlin" "${TZDIR_LOCAL}/posix/Europe/Berlin" "${TZDIR_LOCAL}/right/Europe/Berlin"

# The name as it comes from the `TZ` variable.
env TZDIR="${TZDIR_LOCAL}" TZ="posix/Europe/Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"
env TZDIR="${TZDIR_LOCAL}" TZ="right/Europe/Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"

# The name as it comes from the path of the file the local time zone points to.
env TZDIR="${TZDIR_LOCAL}" TZ=":${TZDIR_LOCAL}/posix/Europe/Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"
env TZDIR="${TZDIR_LOCAL}" TZ=":${TZDIR_LOCAL}/right/Europe/Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"

# The filesystem also accepts path spellings that a zone name does not have: a `./` component, a
# repeated separator, or a leading `./`. They all point at the same zone file, so they have to end
# up as the zone name the path resolves to, and not be kept verbatim.
env TZDIR="${TZDIR_LOCAL}" TZ="Europe/./Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"
env TZDIR="${TZDIR_LOCAL}" TZ="Europe//Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"
env TZDIR="${TZDIR_LOCAL}" TZ="./Europe/Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"
env TZDIR="${TZDIR_LOCAL}" TZ=":Europe/./Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"
env TZDIR="${TZDIR_LOCAL}" TZ=":Europe//Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"

# A non-canonical spelling of a prefixed name has to lose both the prefix and the spelling.
env TZDIR="${TZDIR_LOCAL}" TZ="posix/./Europe/Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"
env TZDIR="${TZDIR_LOCAL}" TZ="right//Europe/Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"

# A canonical name is unaffected.
env TZDIR="${TZDIR_LOCAL}" TZ="Europe/Berlin" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"

rm -rf "${TZDIR_LOCAL}"
