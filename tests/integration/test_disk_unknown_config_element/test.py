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


def write_disk_configuration(node, extra, default_disk=""):
    node.exec_in_container(
        [
            "bash",
            "-c",
            "cat > {} << 'EOF'\n{}EOF".format(
                CONFIG_PATH,
                CONFIG_TEMPLATE.format(extra=extra, default_disk=default_disk),
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
