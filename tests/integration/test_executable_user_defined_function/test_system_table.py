import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

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

        # Write working UDF config
        node.exec_in_container(
            ["bash", "-c", f"echo '{working_udf_config}' > /etc/clickhouse-server/functions/working_udf.xml"]
        )

        # Write broken UDF config
        node.exec_in_container(
            ["bash", "-c", f"echo '{broken_udf_config}' > /etc/clickhouse-server/functions/broken_udf.xml"]
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
            error_message
        FROM system.user_defined_functions
        WHERE name = 'test_working_udf'
        FORMAT Vertical
        """
    )

    print("LOADED UDF result:")
    print(result)

    assert "name: test_working_udf" in result
    assert "status: SUCCESS" in result
    assert "type: executable" in result
    assert "command: working_script.sh" in result
    assert "format: TabSeparated" in result
    assert "return_type: String" in result
    # Error message should be empty for loaded UDFs
    assert "error_message:" in result

    # Query pool UDF configuration
    result = node.query(
        """
        SELECT
            name,
            type,
            pool_size,
            max_command_execution_time,
            send_chunk_header,
            deterministic,
            argument_names
        FROM system.user_defined_functions
        WHERE name = 'test_working_pool_udf'
        FORMAT Vertical
        """
    )

    print("\nPool UDF configuration:")
    print(result)

    assert "name: test_working_pool_udf" in result
    assert "type: executable_pool" in result
    assert "pool_size: 4" in result
    assert "max_command_execution_time: 30" in result
    assert "send_chunk_header: 1" in result
    assert "deterministic: 1" in result
    assert "argument_names: ['value']" in result


def test_system_user_defined_functions_failed_status(started_cluster):
    """Test querying FAILED UDFs and their error messages"""
    skip_test_msan(node)

    # Query failed UDF
    result = node.query(
        """
        SELECT
            name,
            status,
            error_message,
            error_count,
            last_loading_time
        FROM system.user_defined_functions
        WHERE name = 'test_failed_udf_invalid_type'
        FORMAT Vertical
        """
    )

    print("FAILED UDF result:")
    print(result)

    assert "name: test_failed_udf_invalid_type" in result
    assert "status: FAILED" in result

    # Error message should contain information about the failure
    assert "error_message:" in result
    # The actual error message will mention the invalid type
    assert "InvalidTypeName123" in result or "UNKNOWN_TYPE" in result
    assert len(result) > 100  # Should have substantial error message

    # error_count should be > 0 for failed UDFs
    assert "error_count:" in result


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
        FORMAT Vertical
        """
    )

    print("UDFs by status:")
    print(result)

    # Should have both SUCCESS and FAILED
    assert "status: SUCCESS" in result or "status: 0" in result
    assert "status: FAILED" in result or "status: 1" in result

    # List all failed UDFs with their errors
    result = node.query(
        """
        SELECT
            name,
            status,
            substring(error_message, 1, 100) as error_preview
        FROM system.user_defined_functions
        WHERE status = 'FAILED'
        FORMAT Vertical
        """
    )

    print("\nAll FAILED UDFs:")
    print(result)

    assert "test_failed_udf_invalid_type" in result


def test_system_user_defined_functions_columns(started_cluster):
    """Verify all expected columns exist"""
    skip_test_msan(node)

    result = node.query("DESCRIBE system.user_defined_functions")

    print("Table schema:")
    print(result)

    # Core status columns
    assert "name\t" in result
    assert "status\t" in result
    assert "error_message\t" in result
    assert "last_loading_time\t" in result

    # Configuration columns
    assert "type\t" in result
    assert "command\t" in result
    assert "format\t" in result
    assert "return_type\t" in result
    assert "return_name\t" in result
    assert "argument_types\t" in result
    assert "argument_names\t" in result

    # Execution parameter columns
    assert "max_command_execution_time\t" in result
    assert "command_termination_timeout\t" in result
    assert "command_read_timeout\t" in result
    assert "command_write_timeout\t" in result
    assert "pool_size\t" in result
    assert "send_chunk_header\t" in result
    assert "execute_direct\t" in result

    # Additional metadata
    assert "lifetime\t" in result
    assert "deterministic\t" in result
    assert "loading_duration\t" in result
    assert "error_count\t" in result
