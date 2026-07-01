import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=["config/driver_paths.xml", "config/gate.xml"],
)

GATE_PATH = "/etc/clickhouse-server/config.d/gate.xml"

# The driver `create_command` always fails with this marker, so reaching it proves the gate was
# open and the registry had loaded the driver (as opposed to the create being rejected outright).
CREATE_QUERY = (
    "CREATE FUNCTION {name} ARGUMENTS (x UInt64) RETURNS UInt64 "
    "ENGINE = TestDriver() AS 'return x;'"
)


def copy_file_to_container(local_path, dist_path, container_id):
    os.system(
        "docker cp {local} {cont_id}:{dist}".format(
            local=local_path, cont_id=container_id, dist=dist_path
        )
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node.exec_in_container(["mkdir", "-p", "/etc/clickhouse-server/drivers"])
        copy_file_to_container(
            os.path.join(SCRIPT_DIR, "drivers/."),
            "/etc/clickhouse-server/drivers",
            node.docker_id,
        )
        node.exec_in_container(
            ["chmod", "+x", "/etc/clickhouse-server/drivers/test_driver_create.sh"]
        )
        node.exec_in_container(["mkdir", "-p", "/var/lib/clickhouse/dynamic_udf"])

        yield cluster

    finally:
        cluster.shutdown()


def set_gate(value):
    # gate.xml contains a single digit (the gate value), so this replacement is unambiguous.
    node.replace_in_config(
        GATE_PATH,
        "<allow_experimental_executable_udf_drivers>[01]</allow_experimental_executable_udf_drivers>",
        "<allow_experimental_executable_udf_drivers>{}</allow_experimental_executable_udf_drivers>".format(
            value
        ),
    )
    node.query("SYSTEM RELOAD CONFIG")


def test_gate_honored_on_reload(started_cluster):
    # The gate is disabled at startup: creating a driver-based function must be rejected before the
    # driver is ever invoked.
    error = node.query_and_get_error(CREATE_QUERY.format(name="udf_driver_reload_off"))
    assert "allow_experimental_executable_udf_drivers" in error
    assert "TEST_DRIVER_INVOKED" not in error

    # Enabling the gate and reloading the config must both load the driver registry and let the
    # create path proceed: the driver is now invoked and fails with its marker.
    set_gate(1)
    error = node.query_and_get_error(CREATE_QUERY.format(name="udf_driver_reload_on"))
    assert "TEST_DRIVER_INVOKED" in error

    # Disabling the gate and reloading must clear the registry and reject creation again.
    set_gate(0)
    error = node.query_and_get_error(
        CREATE_QUERY.format(name="udf_driver_reload_off_again")
    )
    assert "allow_experimental_executable_udf_drivers" in error
    assert "TEST_DRIVER_INVOKED" not in error

    # Restore the original gate value for a clean module teardown.
    set_gate(0)
