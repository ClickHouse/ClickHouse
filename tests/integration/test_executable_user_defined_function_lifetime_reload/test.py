import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


def skip_test_msan(instance):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with vfork")


config = """<clickhouse>
    <user_defined_executable_functions_config>/etc/clickhouse-server/functions/version_udf_function.xml</user_defined_executable_functions_config>
</clickhouse>"""


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

        copy_file_to_container(
            os.path.join(SCRIPT_DIR, "functions/."),
            "/etc/clickhouse-server/functions",
            node.docker_id,
        )
        copy_file_to_container(
            os.path.join(SCRIPT_DIR, "user_scripts/."),
            "/var/lib/clickhouse/user_scripts",
            node.docker_id,
        )

        node.replace_config(
            "/etc/clickhouse-server/config.d/executable_user_defined_functions_config.xml",
            config,
        )

        node.restart_clickhouse()

        yield cluster

    finally:
        cluster.shutdown()


def write_script(version):
    script = "#!/bin/bash\n\nwhile read read_data;\n    do printf '%s\\n';\ndone\n" % version
    node.exec_in_container(
        [
            "bash",
            "-c",
            "cat > /var/lib/clickhouse/user_scripts/version_udf.sh << 'EOF'\n%sEOF" % script,
        ]
    )
    node.exec_in_container(
        ["chmod", "+x", "/var/lib/clickhouse/user_scripts/version_udf.sh"]
    )


def test_lifetime_reloads_executable_pool_script(started_cluster):
    skip_test_msan(node)

    # The pool UDF starts with the script printing "v1".
    assert node.query("SELECT version_udf(toUInt64(1))") == "v1\n"

    # Edit the script on disk to print "v2".
    write_script("v2")

    # <lifetime> is 2 seconds. After enough ticks the pool must be refreshed and
    # serve the edited script. Poll generously to avoid timing flakiness.
    expected = "v2\n"
    for _ in range(60):
        if node.query("SELECT version_udf(toUInt64(1))") == expected:
            break
        time.sleep(1)

    assert node.query("SELECT version_udf(toUInt64(1))") == expected
