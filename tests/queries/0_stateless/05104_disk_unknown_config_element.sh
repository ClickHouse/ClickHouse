#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An element of a disk definition that nothing reads does nothing, and it is reported instead of being silently ignored.

ERROR=$(${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE unknown_element (x UInt32) ENGINE = MergeTree ORDER BY x
    SETTINGS disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_DATABASE}_unknown/', lazy_initialization = 1, background_load = 1)
" 2>&1)

echo "$ERROR" | grep -oF "UNKNOWN_ELEMENT_IN_CONFIG" | head -n 1
# Both of them are reported, and in the order they are written in.
echo "$ERROR" | grep -oF "lazy_initialization, background_load" | head -n 1

# The same disk without these elements is fine.

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE known_elements (x UInt32) ENGINE = MergeTree ORDER BY x
    SETTINGS disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_DATABASE}_known/', keep_free_space_bytes = 1024);
    INSERT INTO known_elements VALUES (1);
    SELECT * FROM known_elements;
    DROP TABLE known_elements SYNC;
"

# A section of a disk definition is not blessed as a whole: an unknown element inside it is reported as well.

CONFIG="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_locations.xml"

write_config()
{
    cat > "${CONFIG}" <<XML
<clickhouse>
    <storage_configuration>
        <disks>
            <multiple_locations>
                <type>object_storage</type>
                <metadata_type>local</metadata_type>
                <locations>
                    <one>
                        <local>true</local>
                        <enabled>true</enabled>
                        <object_storage_type>local</object_storage_type>
                        <path>${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_one/</path>
                        $1
                    </one>
                </locations>
            </multiple_locations>
        </disks>
    </storage_configuration>
</clickhouse>
XML
}

write_config "<metdata_path>${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_typo/</metdata_path>"
${CLICKHOUSE_LOCAL} --config-file="${CONFIG}" --path="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_db" \
    -q "SELECT name FROM system.disks WHERE name = 'multiple_locations'" 2>&1 \
    | grep -oF "locations.one.metdata_path" | head -n 1

# The same configuration without the unknown element works.

write_config ""
${CLICKHOUSE_LOCAL} --config-file="${CONFIG}" --path="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_db" \
    -q "SELECT name FROM system.disks WHERE name = 'multiple_locations'"

rm -rf "${CONFIG}" "${CLICKHOUSE_TMP:?}/${CLICKHOUSE_DATABASE}_db" "${CLICKHOUSE_TMP:?}/${CLICKHOUSE_DATABASE}_one"
