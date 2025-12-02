import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, main_configs=[])


def skip_test_msan(instance):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with vfork")


def copy_file_to_container(local_path, dist_path, container_id):
    os.system(
        "docker cp {local} {cont_id}:{dist}".format(
            local=local_path, cont_id=container_id, dist=dist_path
        )
    )


# Config with both working and broken UDFs
config = """<clickhouse>
    <user_defined_executable_functions_config>/etc/clickhouse-server/functions/*.xml</user_defined_executable_functions_config>
</clickhouse>"""

# Working UDF config
working_udf_config = """<functions>
    <function>
        <type>executable</type>
        <name>test_working_udf</name>
        <return_type>String</return_type>
        <argument>
            <type>UInt64</type>
        </argument>
        <format>TabSeparated</format>
        <command>working_script.sh</command>
    </function>

    <function>
        <type>executable_pool</type>
        <name>test_working_pool_udf</name>
        <return_type>String</return_type>
        <argument>
            <type>UInt64</type>
            <name>value</name>
        </argument>
        <format>JSONEachRow</format>
        <command>working_script.sh</command>
        <pool_size>4</pool_size>
        <max_command_execution_time>30</max_command_execution_time>
        <send_chunk_header>1</send_chunk_header>
        <deterministic>1</deterministic>
    </function>
</functions>"""

# Broken UDF config - invalid return type causes FAILED status at load time
broken_udf_config = """<functions>
    <function>
        <type>executable</type>
        <name>test_failed_udf_invalid_type</name>
        <return_type>InvalidTypeName123</return_type>
        <argument>
            <type>String</type>
        </argument>
        <format>TabSeparated</format>
        <command>working_script.sh</command>
    </function>
</functions>"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node.replace_config(
            "/etc/clickhouse-server/config.d/executable_user_defined_functions_config.xml",
            config,
        )

        # Create functions directory if it doesn't exist
        node.exec_in_container(
            ["bash", "-c", "mkdir -p /etc/clickhouse-server/functions"]
        )

        # Write working UDF config
        node.exec_in_container(
            ["bash", "-c", f"echo '{working_udf_config}' > /etc/clickhouse-server/functions/working_udf.xml"]
        )

        # Write broken UDF config
        node.exec_in_container(
            ["bash", "-c", f"echo '{broken_udf_config}' > /etc/clickhouse-server/functions/broken_udf.xml"]
        )

        # Create user_scripts directory if it doesn't exist
        node.exec_in_container(
            ["bash", "-c", "mkdir -p /var/lib/clickhouse/user_scripts"]
        )

        # Create working script
        node.exec_in_container(
            ["bash", "-c", "echo '#!/bin/bash\nwhile read line; do echo \"Result: $line\"; done' > /var/lib/clickhouse/user_scripts/working_script.sh"]
        )
        node.exec_in_container(
            ["bash", "-c", "chmod +x /var/lib/clickhouse/user_scripts/working_script.sh"]
        )

        node.restart_clickhouse()

        yield cluster

    finally:
        cluster.shutdown()


def test_system_user_defined_functions_loaded_status(started_cluster):
    """Test querying LOADED UDFs and their configuration"""
    skip_test_msan(node)

    # Query working UDF with LOADED status
    result = node.query(
        """
        SELECT
            name,
            status,
            type,
            command,
            format,
            return_type,
            loading_error_message
        FROM system.user_defined_functions
        WHERE name = 'test_working_udf'
        FORMAT TSV
        """
    )

    print("LOADED UDF result:")
    print(result)

    expected = "test_working_udf\tSUCCESS\texecutable\tworking_script.sh\tTabSeparated\tString\t"
    assert result.strip() == expected.strip()

    # Query pool UDF configuration with more deterministic fields
    result = node.query(
        """
        SELECT
            name,
            status,
            type,
            format,
            return_type,
            argument_types,
            argument_names,
            pool_size,
            max_command_execution_time,
            send_chunk_header,
            deterministic
        FROM system.user_defined_functions
        WHERE name = 'test_working_pool_udf'
        FORMAT TSV
        """
    )

    print("\nPool UDF configuration:")
    print(result)

    assert TSV(result) == TSV([["test_working_pool_udf", "SUCCESS", "executable_pool", "JSONEachRow", "String", "['UInt64']", "['value']", 4, 30, 1, 1]])


def test_system_user_defined_functions_failed_status(started_cluster):
    """Test querying FAILED UDFs and their error messages"""
    skip_test_msan(node)

    # Query failed UDF - check specific fields
    result = node.query(
        """
        SELECT
            name,
            status,
            error_count > 0 AS has_errors,
            position(loading_error_message, 'InvalidTypeName123') > 0 OR position(loading_error_message, 'UNKNOWN_TYPE') > 0 AS has_error_info
        FROM system.user_defined_functions
        WHERE name = 'test_failed_udf_invalid_type'
        FORMAT TSV
        """
    )

    print("FAILED UDF result:")
    print(result)

    expected = "test_failed_udf_invalid_type\tFAILED\t1\t1"
    assert result.strip() == expected.strip()


def test_system_user_defined_functions_list_all_statuses(started_cluster):
    """Test listing all UDFs grouped by status"""
    skip_test_msan(node)

    # Count by status
    result = node.query(
        """
        SELECT
            status,
            count() as cnt
        FROM system.user_defined_functions
        GROUP BY status
        ORDER BY status
        FORMAT TSV
        """
    )

    print("UDFs by status:")
    print(result)

    # Should have both SUCCESS (2 working UDFs) and FAILED (1 broken UDF)
    # Note: ORDER BY status sorts by enum value: SUCCESS(0) < FAILED(1)
    assert TSV(result) == TSV([["SUCCESS", 2], ["FAILED", 1]])

    # List all failed UDFs
    result = node.query(
        """
        SELECT name
        FROM system.user_defined_functions
        WHERE status = 'FAILED'
        FORMAT TSV
        """
    )

    print("\nAll FAILED UDFs:")
    print(result)

    assert result.strip() == "test_failed_udf_invalid_type"


def test_system_user_defined_functions_columns(started_cluster):
    """Verify all expected columns exist"""
    skip_test_msan(node)

    result = node.query(
        """
        SELECT name
        FROM system.columns
        WHERE database = 'system' AND table = 'user_defined_functions'
        ORDER BY name
        FORMAT TSV
        """
    )

    print("Table columns:")
    print(result)

    expected_columns = [
        "argument_names",
        "argument_types",
        "command",
        "command_read_timeout",
        "command_termination_timeout",
        "command_write_timeout",
        "deterministic",
        "error_count",
        "execute_direct",
        "format",
        "last_loading_time",
        "last_successful_update_time",
        "lifetime",
        "loading_duration",
        "loading_error_message",
        "max_command_execution_time",
        "name",
        "pool_size",
        "return_name",
        "return_type",
        "send_chunk_header",
        "status",
        "type",
    ]

    actual_columns = result.strip().split('\n')
    assert actual_columns == expected_columns
