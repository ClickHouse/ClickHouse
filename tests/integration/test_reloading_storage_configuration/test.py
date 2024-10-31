import collections
import os
import re
import shutil
import time
import xml.etree.ElementTree as ET

import helpers.client
import helpers.cluster
from helpers.test_tools import TSV
import pytest


cluster = helpers.cluster.ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/logs_config.xml"],
    with_zookeeper=True,
    stay_alive=True,
    tmpfs=[
        "/jbod1:size=40M",
        "/jbod2:size=40M",
        "/jbod3:size=40M",
        "/jbod4:size=40M",
        "/external:size=200M",
    ],
    macros={"shard": 0, "replica": 1},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/logs_config.xml"],
    with_zookeeper=True,
    stay_alive=True,
    tmpfs=[
        "/jbod1:size=40M",
        "/jbod2:size=40M",
        "/jbod3:size=40M",
        "/jbod4:size=40M",
        "/external:size=200M",
    ],
    macros={"shard": 0, "replica": 2},
)


def get_log(node):
    return node.exec_in_container(
        ["bash", "-c", "cat /var/log/clickhouse-server/clickhouse-server.log"]
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def start_over():
    shutil.copy(
        os.path.join(
            os.path.dirname(__file__), "configs/config.d/storage_configuration.xml"
        ),
        os.path.join(node1.config_d_dir, "storage_configuration.xml"),
    )

    for node in (node1, node2):
        separate_configuration_path = os.path.join(
            node.config_d_dir, "separate_configuration.xml"
        )
        try:
            os.remove(separate_configuration_path)
        except:
            """"""


def add_disk(node, name, path, separate_file=False):
    separate_configuration_path = os.path.join(
        node.config_d_dir, "separate_configuration.xml"
    )

    try:
        if separate_file:
            tree = ET.parse(separate_configuration_path)
        else:
            tree = ET.parse(
                os.path.join(node.config_d_dir, "storage_configuration.xml")
            )
    except:
        tree = ET.ElementTree(
            ET.fromstring(
                "<clickhouse><storage_configuration><disks/><policies/></storage_configuration></clickhouse>"
            )
        )
    root = tree.getroot()
    new_disk = ET.Element(name)
    new_path = ET.Element("path")
    new_path.text = path
    new_disk.append(new_path)
    root.find("storage_configuration").find("disks").append(new_disk)
    if separate_file:
        tree.write(separate_configuration_path)
    else:
        tree.write(os.path.join(node.config_d_dir, "storage_configuration.xml"))


def update_disk(node, name, path, keep_free_space_bytes, separate_file=False):
    separate_configuration_path = os.path.join(
        node.config_d_dir, "separate_configuration.xml"
    )

    try:
        if separate_file:
            tree = ET.parse(separate_configuration_path)
        else:
            tree = ET.parse(
                os.path.join(node.config_d_dir, "storage_configuration.xml")
            )
    except:
        tree = ET.ElementTree(
            ET.fromstring(
                "<clickhouse><storage_configuration><disks/><policies/></storage_configuration></clickhouse>"
            )
        )

    root = tree.getroot()
    disk = root.find("storage_configuration").find("disks").find(name)
    assert disk is not None

    new_path = disk.find("path")
    assert new_path is not None
    new_path.text = path

    new_keep_free_space_bytes = disk.find("keep_free_space_bytes")
    assert new_keep_free_space_bytes is not None
    new_keep_free_space_bytes.text = keep_free_space_bytes

    if separate_file:
        tree.write(separate_configuration_path)
    else:
        tree.write(os.path.join(node.config_d_dir, "storage_configuration.xml"))


def add_policy(node, name, volumes):
    tree = ET.parse(os.path.join(node.config_d_dir, "storage_configuration.xml"))
    root = tree.getroot()
    new_policy = ET.Element(name)
    new_volumes = ET.Element("volumes")
    for volume, disks in list(volumes.items()):
        new_volume = ET.Element(volume)
        for disk in disks:
            new_disk = ET.Element("disk")
            new_disk.text = disk
            new_volume.append(new_disk)
        new_volumes.append(new_volume)
    new_policy.append(new_volumes)
    root.find("storage_configuration").find("policies").append(new_policy)
    tree.write(os.path.join(node.config_d_dir, "storage_configuration.xml"))


def test_add_disk(started_cluster):
    try:
        name = "test_add_disk"
        engine = "MergeTree()"

        start_over()
        node1.restart_clickhouse(kill=True)
        time.sleep(2)

        node1.query(
            """
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        assert "jbod3" not in set(
            node1.query("SELECT name FROM system.disks").splitlines()
        )

        add_disk(node1, "jbod3", "/jbod3/")
        node1.query("SYSTEM RELOAD CONFIG")

        assert "jbod3" in set(node1.query("SELECT name FROM system.disks").splitlines())
    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            """"""


def test_update_disk(started_cluster):
    try:
        name = "test_update_disk"
        engine = "MergeTree()"

        start_over()
        node1.restart_clickhouse(kill=True)
        time.sleep(2)

        node1.query(
            """
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        assert node1.query(
            "SELECT path, keep_free_space FROM system.disks where name = 'jbod2'"
        ) == TSV([["/jbod2/", "10485760"]])

        update_disk(node1, "jbod2", "/jbod2/", "20971520")
        node1.query("SYSTEM RELOAD CONFIG")

        assert node1.query(
            "SELECT path, keep_free_space FROM system.disks where name = 'jbod2'"
        ) == TSV([["/jbod2/", "20971520"]])
    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            """"""


def test_add_disk_to_separate_config(started_cluster):
    try:
        name = "test_add_disk"
        engine = "MergeTree()"

        start_over()
        node1.restart_clickhouse(kill=True)
        time.sleep(2)

        node1.query(
            """
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        assert "jbod3" not in set(
            node1.query("SELECT name FROM system.disks").splitlines()
        )

        add_disk(node1, "jbod3", "/jbod3/", separate_file=True)
        node1.query("SYSTEM RELOAD CONFIG")

        assert "jbod3" in set(node1.query("SELECT name FROM system.disks").splitlines())
        start_over()

    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            """"""


def test_add_policy(started_cluster):
    try:
        name = "test_add_policy"
        engine = "MergeTree()"

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        node1.restart_clickhouse(kill=True)
        time.sleep(2)

        node1.query(
            """
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        add_policy(node1, "cool_policy", {"volume1": ["jbod3", "jbod4"]})
        node1.query("SYSTEM RELOAD CONFIG")

        disks = set(node1.query("SELECT name FROM system.disks").splitlines())
        assert "cool_policy" in set(
            node1.query("SELECT policy_name FROM system.storage_policies").splitlines()
        )
        assert {"volume1"} == set(
            node1.query(
                "SELECT volume_name FROM system.storage_policies WHERE policy_name = 'cool_policy'"
            ).splitlines()
        )
        assert {"['jbod3','jbod4']"} == set(
            node1.query(
                "SELECT disks FROM system.storage_policies WHERE policy_name = 'cool_policy'"
            ).splitlines()
        )

    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            """"""


def test_new_policy_works(started_cluster):
    try:
        name = "test_new_policy_works"
        engine = "MergeTree()"

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        node1.restart_clickhouse(kill=True)
        time.sleep(2)

        node1.query(
            """
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        add_policy(node1, "cool_policy", {"volume1": ["jbod3"]})
        node1.query("SYSTEM RELOAD CONFIG")

        # Incompatible storage policy.
        with pytest.raises(helpers.client.QueryRuntimeException):
            node1.query(
                """
                ALTER TABLE {name} MODIFY SETTING storage_policy='cool_policy'
            """.format(
                    name=name
                )
            )

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        add_policy(
            node1,
            "cool_policy",
            collections.OrderedDict(
                [
                    ("volume1", ["jbod3"]),
                    ("main", ["jbod1", "jbod2"]),
                    ("external", ["external"]),
                ]
            ),
        )
        node1.query("SYSTEM RELOAD CONFIG")

        node1.query(
            """
            ALTER TABLE {name} MODIFY SETTING storage_policy='cool_policy'
        """.format(
                name=name
            )
        )

        node1.query(
            """
            INSERT INTO TABLE {name} VALUES (1)
        """.format(
                name=name
            )
        )
        assert {"jbod3"} == set(
            node1.query(
                "SELECT disk_name FROM system.parts WHERE active = 1 AND table = '{name}'".format(
                    name=name
                )
            ).splitlines()
        )

    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            """"""


def test_add_volume_to_policy(started_cluster):
    try:
        name = "test_add_volume_to_policy"
        engine = "MergeTree()"

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        add_policy(node1, "cool_policy", {"volume1": ["jbod3"]})
        node1.restart_clickhouse(kill=True)
        time.sleep(2)

        node1.query(
            """
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        add_policy(
            node1,
            "cool_policy",
            collections.OrderedDict([("volume1", ["jbod3"]), ("volume2", ["jbod4"])]),
        )
        node1.query("SYSTEM RELOAD CONFIG")

        volumes = set(
            node1.query(
                "SELECT volume_name FROM system.storage_policies WHERE policy_name = 'cool_policy'"
            ).splitlines()
        )
        disks_sets = set(
            node1.query(
                "SELECT disks FROM system.storage_policies WHERE policy_name = 'cool_policy'"
            ).splitlines()
        )
        assert {"volume1", "volume2"} == volumes
        assert {"['jbod3']", "['jbod4']"} == disks_sets

    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            """"""


def test_add_disk_to_policy(started_cluster):
    try:
        name = "test_add_disk_to_policy"
        engine = "MergeTree()"

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        add_policy(node1, "cool_policy", {"volume1": ["jbod3"]})
        node1.restart_clickhouse(kill=True)
        time.sleep(2)

        node1.query(
            """
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        add_policy(node1, "cool_policy", {"volume1": ["jbod3", "jbod4"]})
        node1.query("SYSTEM RELOAD CONFIG")

        volumes = set(
            node1.query(
                "SELECT volume_name FROM system.storage_policies WHERE policy_name = 'cool_policy'"
            ).splitlines()
        )
        disks_sets = set(
            node1.query(
                "SELECT disks FROM system.storage_policies WHERE policy_name = 'cool_policy'"
            ).splitlines()
        )
        assert {"volume1"} == volumes
        assert {"['jbod3','jbod4']"} == disks_sets

    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            """"""


def test_remove_disk(started_cluster):
    try:
        name = "test_remove_disk"
        engine = "MergeTree()"

        start_over()
        add_disk(node1, "remove_disk_jbod3", "/jbod3/")
        node1.restart_clickhouse(kill=True)
        time.sleep(2)

        node1.query(
            """
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        assert "remove_disk_jbod3" in set(
            node1.query("SELECT name FROM system.disks").splitlines()
        )

        start_over()
        node1.query("SYSTEM RELOAD CONFIG")

        assert "remove_disk_jbod3" in set(
            node1.query("SELECT name FROM system.disks").splitlines()
        )
        assert re.search("Warning.*remove_disk_jbod3", get_log(node1))
    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            """"""


def test_remove_policy(started_cluster):
    try:
        name = "test_remove_policy"
        engine = "MergeTree()"

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        add_policy(node1, "remove_policy_cool_policy", {"volume1": ["jbod3", "jbod4"]})
        node1.restart_clickhouse(kill=True)
        time.sleep(2)

        node1.query(
            """
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        assert "remove_policy_cool_policy" in set(
            node1.query("SELECT policy_name FROM system.storage_policies").splitlines()
        )

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        node1.query("SYSTEM RELOAD CONFIG")

        assert "remove_policy_cool_policy" in set(
            node1.query("SELECT policy_name FROM system.storage_policies").splitlines()
        )
        assert re.search("Error.*remove_policy_cool_policy", get_log(node1))

    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            """"""


def test_remove_volume_from_policy(started_cluster):
    try:
        name = "test_remove_volume_from_policy"
        engine = "MergeTree()"

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        add_policy(
            node1,
            "test_remove_volume_from_policy_cool_policy",
            collections.OrderedDict([("volume1", ["jbod3"]), ("volume2", ["jbod4"])]),
        )
        node1.restart_clickhouse(kill=True)
        time.sleep(2)

        node1.query(
            """
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        volumes = set(
            node1.query(
                "SELECT volume_name FROM system.storage_policies WHERE policy_name = 'test_remove_volume_from_policy_cool_policy'"
            ).splitlines()
        )
        disks_sets = set(
            node1.query(
                "SELECT disks FROM system.storage_policies WHERE policy_name = 'test_remove_volume_from_policy_cool_policy'"
            ).splitlines()
        )
        assert {"volume1", "volume2"} == volumes
        assert {"['jbod3']", "['jbod4']"} == disks_sets

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        add_policy(node1, "cool_policy", {"volume1": ["jbod3"]})
        node1.query("SYSTEM RELOAD CONFIG")

        volumes = set(
            node1.query(
                "SELECT volume_name FROM system.storage_policies WHERE policy_name = 'test_remove_volume_from_policy_cool_policy'"
            ).splitlines()
        )
        disks_sets = set(
            node1.query(
                "SELECT disks FROM system.storage_policies WHERE policy_name = 'test_remove_volume_from_policy_cool_policy'"
            ).splitlines()
        )
        assert {"volume1", "volume2"} == volumes
        assert {"['jbod3']", "['jbod4']"} == disks_sets
        assert re.search(
            "Error.*test_remove_volume_from_policy_cool_policy", get_log(node1)
        )

    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            """"""


def test_remove_disk_from_policy(started_cluster):
    try:
        name = "test_remove_disk_from_policy"
        engine = "MergeTree()"

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        add_policy(
            node1,
            "test_remove_disk_from_policy_cool_policy",
            {"volume1": ["jbod3", "jbod4"]},
        )
        node1.restart_clickhouse(kill=True)
        time.sleep(2)

        node1.query(
            """
            CREATE TABLE {name} (
                d UInt64
            ) ENGINE = {engine}
            ORDER BY d
            SETTINGS storage_policy='jbods_with_external'
        """.format(
                name=name, engine=engine
            )
        )

        volumes = set(
            node1.query(
                "SELECT volume_name FROM system.storage_policies WHERE policy_name = 'test_remove_disk_from_policy_cool_policy'"
            ).splitlines()
        )
        disks_sets = set(
            node1.query(
                "SELECT disks FROM system.storage_policies WHERE policy_name = 'test_remove_disk_from_policy_cool_policy'"
            ).splitlines()
        )
        assert {"volume1"} == volumes
        assert {"['jbod3','jbod4']"} == disks_sets

        start_over()
        add_disk(node1, "jbod3", "/jbod3/")
        add_disk(node1, "jbod4", "/jbod4/")
        add_policy(node1, "cool_policy", {"volume1": ["jbod3"]})
        node1.query("SYSTEM RELOAD CONFIG")

        volumes = set(
            node1.query(
                "SELECT volume_name FROM system.storage_policies WHERE policy_name = 'test_remove_disk_from_policy_cool_policy'"
            ).splitlines()
        )
        disks_sets = set(
            node1.query(
                "SELECT disks FROM system.storage_policies WHERE policy_name = 'test_remove_disk_from_policy_cool_policy'"
            ).splitlines()
        )
        assert {"volume1"} == volumes
        assert {"['jbod3','jbod4']"} == disks_sets
        assert re.search(
            "Error.*test_remove_disk_from_policy_cool_policy", get_log(node1)
        )

    finally:
        try:
            node1.query("DROP TABLE IF EXISTS {}".format(name))
        except:
            """"""
