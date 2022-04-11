import os

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        for i in range(2, 8):
            node.exec_in_container(
                [
                    "cp",
                    "/etc/clickhouse-server/users.xml",
                    "/etc/clickhouse-server/users{}.xml".format(i),
                ]
            )

        yield cluster

    finally:
        cluster.shutdown()


def test_old_style():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/old_style.xml"),
        "/etc/clickhouse-server/config.d/z.xml",
    )
    node.restart_clickhouse()
    assert node.query("SELECT * FROM system.user_directories") == TSV(
        [
            [
                "users.xml",
                "users.xml",
                '{"path":"\\\\/etc\\\\/clickhouse-server\\\\/users2.xml"}',
                1,
            ],
            [
                "local directory",
                "local directory",
                '{"path":"\\\\/var\\\\/lib\\\\/clickhouse\\\\/access2\\\\/"}',
                2,
            ],
        ]
    )


def test_local_directories():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/local_directories.xml"),
        "/etc/clickhouse-server/config.d/z.xml",
    )
    node.restart_clickhouse()
    assert node.query("SELECT * FROM system.user_directories") == TSV(
        [
            [
                "users.xml",
                "users.xml",
                '{"path":"\\\\/etc\\\\/clickhouse-server\\\\/users3.xml"}',
                1,
            ],
            [
                "local directory",
                "local directory",
                '{"path":"\\\\/var\\\\/lib\\\\/clickhouse\\\\/access3\\\\/"}',
                2,
            ],
            [
                "local directory (ro)",
                "local directory",
                '{"path":"\\\\/var\\\\/lib\\\\/clickhouse\\\\/access3-ro\\\\/","readonly":true}',
                3,
            ],
        ]
    )


def test_relative_path():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/relative_path.xml"),
        "/etc/clickhouse-server/config.d/z.xml",
    )
    node.restart_clickhouse()
    assert node.query("SELECT * FROM system.user_directories") == TSV(
        [
            [
                "users.xml",
                "users.xml",
                '{"path":"\\\\/etc\\\\/clickhouse-server\\\\/users4.xml"}',
                1,
            ]
        ]
    )


def test_memory():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/memory.xml"),
        "/etc/clickhouse-server/config.d/z.xml",
    )
    node.restart_clickhouse()
    assert node.query("SELECT * FROM system.user_directories") == TSV(
        [
            [
                "users.xml",
                "users.xml",
                '{"path":"\\\\/etc\\\\/clickhouse-server\\\\/users5.xml"}',
                1,
            ],
            ["memory", "memory", "{}", 2],
        ]
    )


def test_mixed_style():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/mixed_style.xml"),
        "/etc/clickhouse-server/config.d/z.xml",
    )
    node.restart_clickhouse()
    assert node.query("SELECT * FROM system.user_directories") == TSV(
        [
            [
                "users.xml",
                "users.xml",
                '{"path":"\\\\/etc\\\\/clickhouse-server\\\\/users6.xml"}',
                1,
            ],
            [
                "local directory",
                "local directory",
                '{"path":"\\\\/var\\\\/lib\\\\/clickhouse\\\\/access6\\\\/"}',
                2,
            ],
            [
                "local directory",
                "local directory",
                '{"path":"\\\\/var\\\\/lib\\\\/clickhouse\\\\/access6a\\\\/"}',
                3,
            ],
            ["memory", "memory", "{}", 4],
        ]
    )


def test_duplicates():
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/duplicates.xml"),
        "/etc/clickhouse-server/config.d/z.xml",
    )
    node.restart_clickhouse()
    assert node.query("SELECT * FROM system.user_directories") == TSV(
        [
            [
                "users.xml",
                "users.xml",
                '{"path":"\\\\/etc\\\\/clickhouse-server\\\\/users7.xml"}',
                1,
            ],
            [
                "local directory",
                "local directory",
                '{"path":"\\\\/var\\\\/lib\\\\/clickhouse\\\\/access7\\\\/"}',
                2,
            ],
        ]
    )
