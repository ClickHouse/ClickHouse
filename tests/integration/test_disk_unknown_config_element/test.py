import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

CONFIG_PATH = "/etc/clickhouse-server/config.d/storage_configuration.xml"

CONFIG_TEMPLATE = """<clickhouse>
    <storage_configuration>
        <disks>
            {default_disk}
            <checked_disk>
                <type>local</type>
                <path>/var/lib/clickhouse/disks/checked_disk/</path>
                {extra}
            </checked_disk>
            <locations_disk>
                <type>object_storage</type>
                <metadata_type>local</metadata_type>
                <metadata_path>/var/lib/clickhouse/disks/locations_disk_metadata/</metadata_path>
                <locations>
                    <main>
                        <object_storage_type>local</object_storage_type>
                        <path>/var/lib/clickhouse/disks/locations_disk/</path>
                        <local>1</local>
                        <enabled>1</enabled>
                        {location_extra}
                    </main>
                </locations>
            </locations_disk>
            <encrypted_disk>
                <type>encrypted</type>
                <disk>checked_disk</disk>
                <path>encrypted/</path>
                {encrypted_keys}
            </encrypted_disk>
            <s3_disk>
                <type>s3</type>
                <endpoint>http://127.0.0.1:1/bucket/data/</endpoint>
                <access_key_id>a</access_key_id>
                <secret_access_key>b</secret_access_key>
                <metadata_path>/var/lib/clickhouse/disks/s3_disk_metadata/</metadata_path>
                <skip_access_check>1</skip_access_check>
                {s3_extra}
            </s3_disk>
        </disks>
    </storage_configuration>
</clickhouse>
"""


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.add_instance(
            "node",
            main_configs=["configs/config.d/storage_configuration.xml"],
            stay_alive=True,
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


DEFAULT_ENCRYPTED_KEYS = "<key>firstfirstfirstf</key>"


def write_disk_configuration(
    node,
    extra,
    default_disk="",
    location_extra="",
    encrypted_keys=DEFAULT_ENCRYPTED_KEYS,
    s3_extra="",
):
    node.exec_in_container(
        [
            "bash",
            "-c",
            "cat > {} << 'EOF'\n{}EOF".format(
                CONFIG_PATH,
                CONFIG_TEMPLATE.format(
                    extra=extra,
                    default_disk=default_disk,
                    location_extra=location_extra,
                    encrypted_keys=encrypted_keys,
                    s3_extra=s3_extra,
                ),
            ),
        ]
    )


def test_unknown_element_is_reported_on_reload(start_cluster):
    node = cluster.instances["node"]
    assert "checked_disk" in node.query("SELECT name FROM system.disks")

    # An option of this disk type that was not in the section before is accepted.
    write_disk_configuration(
        node, "<keep_free_space_bytes>1024</keep_free_space_bytes>"
    )
    node.query("SYSTEM RELOAD CONFIG")
    assert "checked_disk" in node.query("SELECT name FROM system.disks")

    # A typo added by the reload is reported, and the previous configuration keeps working.
    write_disk_configuration(node, "<keep_free_space_byte>1024</keep_free_space_byte>")
    error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in error
    assert "keep_free_space_byte" in error
    assert "checked_disk" in node.query("SELECT name FROM system.disks")

    # And the server recovers once the typo is removed.
    write_disk_configuration(
        node, "<keep_free_space_bytes>1024</keep_free_space_bytes>"
    )
    node.query("SYSTEM RELOAD CONFIG")
    assert "checked_disk" in node.query("SELECT name FROM system.disks")


def keep_free_space(node, disk):
    return node.query(
        f"SELECT keep_free_space FROM system.disks WHERE name = '{disk}'"
    ).strip()


def test_rejected_reload_does_not_change_the_disk(start_cluster):
    node = cluster.instances["node"]

    write_disk_configuration(
        node, "<keep_free_space_bytes>1024</keep_free_space_bytes>"
    )
    node.query("SYSTEM RELOAD CONFIG")
    assert keep_free_space(node, "checked_disk") == "1024"

    # A real change next to a typo: the whole definition is rejected, nothing of it is applied.
    write_disk_configuration(
        node,
        "<keep_free_space_bytes>2048</keep_free_space_bytes>"
        "<keep_free_space_byte>2048</keep_free_space_byte>",
    )
    error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in error
    assert keep_free_space(node, "checked_disk") == "1024"

    write_disk_configuration(
        node, "<keep_free_space_bytes>1024</keep_free_space_bytes>"
    )
    node.query("SYSTEM RELOAD CONFIG")


def test_section_added_for_implicit_default_disk(start_cluster):
    node = cluster.instances["node"]

    # The `default` disk is created implicitly, without a section; a section added later is checked too.
    write_disk_configuration(
        node,
        "",
        "<default><keep_free_space_byte>1024</keep_free_space_byte></default>",
    )
    error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in error
    assert "keep_free_space_byte" in error
    assert keep_free_space(node, "default") == "0"

    write_disk_configuration(
        node,
        "",
        "<default><keep_free_space_bytes>1024</keep_free_space_bytes></default>",
    )
    node.query("SYSTEM RELOAD CONFIG")
    assert keep_free_space(node, "default") == "1024"

    write_disk_configuration(node, "")
    node.query("SYSTEM RELOAD CONFIG")


def test_unknown_element_inside_existing_section(start_cluster):
    node = cluster.instances["node"]
    assert "locations_disk" in node.query("SELECT name FROM system.disks")

    # A typo inside a section that the disk was created with is reported as well.
    write_disk_configuration(
        node,
        "<keep_free_space_bytes>1024</keep_free_space_bytes>",
        location_extra="<localy>1</localy>",
    )
    error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in error
    assert "locations.main.localy" in error

    write_disk_configuration(
        node, "<keep_free_space_bytes>1024</keep_free_space_bytes>"
    )
    node.query("SYSTEM RELOAD CONFIG")
    assert "locations_disk" in node.query("SELECT name FROM system.disks")


def test_unknown_element_next_to_encryption_keys(start_cluster):
    node = cluster.instances["node"]
    assert "encrypted_disk" in node.query("SELECT name FROM system.disks")

    rotated_keys = (
        '<key id="0">firstfirstfirstf</key>'
        '<key_hex id="1">00112233445566778899aabbccddeeff</key_hex>'
        "<current_key_id>1</current_key_id>"
    )

    # The keys of an `encrypted` disk are picked by a pattern of their names (`key`, `key[1]`, ...),
    # but an element that matches no pattern, even if its name starts with `key`, is reported
    # before the reload is applied, together with a key rotation.
    write_disk_configuration(
        node,
        "<keep_free_space_bytes>1024</keep_free_space_bytes>",
        encrypted_keys=rotated_keys + "<key_typo>1</key_typo>",
    )
    error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in error
    assert "encrypted_disk" in error
    assert "key_typo" in error

    # A key rotation alone, with a key of a form that was not used before, is accepted.
    write_disk_configuration(
        node,
        "<keep_free_space_bytes>1024</keep_free_space_bytes>",
        encrypted_keys=rotated_keys,
    )
    node.query("SYSTEM RELOAD CONFIG")
    assert "encrypted_disk" in node.query("SELECT name FROM system.disks")

    write_disk_configuration(
        node, "<keep_free_space_bytes>1024</keep_free_space_bytes>"
    )
    node.query("SYSTEM RELOAD CONFIG")


def test_unknown_element_in_new_proxy_section(start_cluster):
    node = cluster.instances["node"]
    assert "s3_disk" in node.query("SELECT name FROM system.disks")

    # The disk was created without a `proxy` section. A new one rebuilds the client of the disk
    # right away, so an unknown element inside it is reported before the reload is applied.
    write_disk_configuration(
        node,
        "<keep_free_space_bytes>1024</keep_free_space_bytes>",
        s3_extra="<proxy><uri>http://proxy1:3128</uri><bad>1</bad></proxy>",
    )
    error = node.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert "UNKNOWN_ELEMENT_IN_CONFIG" in error
    assert "proxy.bad" in error

    # A valid `proxy` section is accepted.
    write_disk_configuration(
        node,
        "<keep_free_space_bytes>1024</keep_free_space_bytes>",
        s3_extra="<proxy><uri>http://proxy1:3128</uri></proxy>",
    )
    node.query("SYSTEM RELOAD CONFIG")
    assert "s3_disk" in node.query("SELECT name FROM system.disks")

    write_disk_configuration(
        node, "<keep_free_space_bytes>1024</keep_free_space_bytes>"
    )
    node.query("SYSTEM RELOAD CONFIG")
