#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

RESOURCE_PREFIX="${CLICKHOUSE_DATABASE}_03232_"

$CLICKHOUSE_CLIENT -q "
CREATE OR REPLACE RESOURCE ${RESOURCE_PREFIX}resource_1 (WRITE DISK ${RESOURCE_PREFIX}disk_1, READ DISK ${RESOURCE_PREFIX}disk_1);
SELECT name, read_disks, write_disks, create_query FROM system.resources WHERE name ILIKE '${RESOURCE_PREFIX}%' ORDER BY name;
CREATE RESOURCE IF NOT EXISTS ${RESOURCE_PREFIX}resource_2 (READ DISK ${RESOURCE_PREFIX}disk_2);
CREATE RESOURCE ${RESOURCE_PREFIX}resource_3 (WRITE DISK ${RESOURCE_PREFIX}disk_2);
SELECT name, read_disks, write_disks, create_query FROM system.resources WHERE name ILIKE '${RESOURCE_PREFIX}%' ORDER BY name;
DROP RESOURCE IF EXISTS ${RESOURCE_PREFIX}resource_2;
DROP RESOURCE ${RESOURCE_PREFIX}resource_3;
SELECT name, read_disks, write_disks, create_query FROM system.resources WHERE name ILIKE '${RESOURCE_PREFIX}%' ORDER BY name;
DROP RESOURCE ${RESOURCE_PREFIX}resource_1;
"