import logging
import os
import os.path as p
import time

import pytest

from helpers.cluster import ClickHouseCluster, run_and_check
from test_library_bridge.test import create_dict_simple

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    dictionaries=["configs/dictionaries/dict1.xml"],
    main_configs=["configs/config.d/config.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def ch_cluster():
    try:
        cluster.start()
        instance.query("CREATE DATABASE test")

        instance.copy_file_to_container(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "configs/dict_lib.cpp"
            ),
            "/etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.cpp",
        )

        instance.query("SYSTEM RELOAD CONFIG")

        instance.exec_in_container(
            [
                "bash",
                "-c",
                "/usr/bin/g++ -shared -o /etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.so -fPIC /etc/clickhouse-server/config.d/dictionaries_lib/dict_lib.cpp",
            ],
            user="root",
        )
        yield cluster

    finally:
        cluster.shutdown()


def test_bridge_dies_with_parent(ch_cluster):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")
    if instance.is_built_with_address_sanitizer():
        pytest.skip(
            "Leak sanitizer falsely reports about a leak of 16 bytes in clickhouse-odbc-bridge"
        )

    create_dict_simple(instance)
    result = instance.query("""select dictGet(lib_dict_c, 'value1', toUInt64(1));""")
    assert result.strip() == "101"

    clickhouse_pid = instance.get_process_pid("clickhouse server")
    bridge_pid = instance.get_process_pid("library-bridge")
    assert clickhouse_pid is not None
    assert bridge_pid is not None

    try:
        instance.exec_in_container(
            ["kill", str(clickhouse_pid)], privileged=True, user="root"
        )
    except:
        pass

    for i in range(30):
        time.sleep(1)
        clickhouse_pid = instance.get_process_pid("clickhouse server")
        if clickhouse_pid is None:
            break

    for i in range(30):
        time.sleep(1)
        bridge_pid = instance.get_process_pid("library-bridge")
        if bridge_pid is None:
            break

    if bridge_pid:
        out = instance.exec_in_container(
            ["gdb", "-p", str(bridge_pid), "--ex", "thread apply all bt", "--ex", "q"],
            privileged=True,
            user="root",
        )
        logging.debug(f"Bridge is running, gdb output:\n{out}")

    try:
        assert clickhouse_pid is None
        assert bridge_pid is None
    finally:
        instance.start_clickhouse(20)
        instance.query("DROP DICTIONARY lib_dict_c")
