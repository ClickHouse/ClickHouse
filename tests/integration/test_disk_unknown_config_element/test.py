import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

CONFIG_PATH = "/etc/clickhouse-server/config.d/storage_configuration.xml"

CONFIG_TEMPLATE = """<clickhouse>
    <storage_configuration>
        <disks>
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


def write_disk_configuration(node, extra):
    node.exec_in_container(
        [
            "bash",
            "-c",
            "cat > {} << 'EOF'\n{}EOF".format(
                CONFIG_PATH, CONFIG_TEMPLATE.format(extra=extra)
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
