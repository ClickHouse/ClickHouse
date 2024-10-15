# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

# This test covers the following options:
# - max_backup_bandwidth
# - max_backup_bandwidth_for_server
# - max_local_read_bandwidth
# - max_local_read_bandwidth_for_server
# - max_local_write_bandwidth
# - max_local_write_bandwidth_for_server
# - max_remote_read_network_bandwidth
# - max_remote_read_network_bandwidth_for_server
# - max_remote_write_network_bandwidth
# - max_remote_write_network_bandwidth_for_server
# - and that max_backup_bandwidth from the query will override setting from the user profile

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


def elapsed(func, *args, **kwargs):
    start = time.time()
    ret = func(*args, **kwargs)
    end = time.time()
    return ret, end - start


node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=[
        "configs/static_overrides.xml",
        "configs/dynamic_overrides.xml",
        "configs/ssl.xml",
    ],
    user_configs=[
        "configs/users_overrides.xml",
        "configs/users_overrides_persistent.xml",
    ],
    with_minio=True,
    minio_certs_dir="minio_certs",
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def revert_config():
    # Revert configs after the test, not before
    yield
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo '<clickhouse></clickhouse>' > /etc/clickhouse-server/config.d/dynamic_overrides.xml",
        ]
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo '<clickhouse></clickhouse>' > /etc/clickhouse-server/users.d/users_overrides.xml",
        ]
    )
    node.restart_clickhouse()


backup_id_counter = 0


def next_backup_name(storage):
    global backup_id_counter
    if storage == "local":
        backup_id_counter += 1
        return f"Disk('default', '{backup_id_counter}/')"
    elif storage == "remote":
        backup_id_counter += 1
        return f"S3(s3, '{backup_id_counter}/')"
    else:
        raise Exception(storage)


def node_update_config(mode, setting, value=None):
    if mode is None:
        return
    if mode == "server":
        config_path = "/etc/clickhouse-server/config.d/dynamic_overrides.xml"
        config_content = f"""
        <clickhouse><{setting}>{value}</{setting}></clickhouse>
        """
    else:
        config_path = "/etc/clickhouse-server/users.d/users_overrides.xml"
        config_content = f"""
        <clickhouse>
            <profiles>
                <default>
                    <{setting}>{value}</{setting}>
                </default>
            </profiles>
        </clickhouse>
        """
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"echo '{config_content}' > {config_path}",
        ]
    )
    node.restart_clickhouse()


def assert_took(took, should_take):
    # we need to decrease the lower limit because the server limits could
    # be enforced by throttling some server background IO instead of query IO
    # and we have no control over it
    assert took >= should_take * 0.85


@pytest.mark.parametrize(
    "policy,backup_name,mode,setting,value,should_take",
    [
        #
        # Local -> Local
        #
        pytest.param(
            "default",
            next_backup_name("local"),
            None,
            None,
            None,
            0,
            id="no_local_throttling",
        ),
        # reading 1e6*8 bytes with 1M default bandwidth should take (8-1)/1=7 seconds
        pytest.param(
            "default",
            next_backup_name("local"),
            "user",
            "max_backup_bandwidth",
            "1M",
            7,
            id="user_local_throttling",
        ),
        # reading 1e6*8 bytes with 2M default bandwidth should take (8-2)/2=3 seconds
        pytest.param(
            "default",
            next_backup_name("local"),
            "server",
            "max_backup_bandwidth_for_server",
            "2M",
            3,
            id="server_local_throttling",
        ),
        #
        # Remote -> Local
        #
        pytest.param(
            "s3",
            next_backup_name("local"),
            None,
            None,
            None,
            0,
            id="no_remote_to_local_throttling",
        ),
        # reading 1e6*8 bytes with 1M default bandwidth should take (8-1)/1=7 seconds
        pytest.param(
            "s3",
            next_backup_name("local"),
            "user",
            "max_backup_bandwidth",
            "1M",
            7,
            id="user_remote_to_local_throttling",
        ),
        # reading 1e6*8 bytes with 2M default bandwidth should take (8-2)/2=3 seconds
        pytest.param(
            "s3",
            next_backup_name("local"),
            "server",
            "max_backup_bandwidth_for_server",
            "2M",
            3,
            id="server_remote_to_local_throttling",
        ),
        #
        # Remote -> Remote
        #
        pytest.param(
            "s3",
            next_backup_name("remote"),
            None,
            None,
            None,
            0,
            id="no_remote_to_remote_throttling",
        ),
        # No throttling for S3-to-S3, uses native copy
        pytest.param(
            "s3",
            next_backup_name("remote"),
            "user",
            "max_backup_bandwidth",
            "1M",
            0,
            id="user_remote_to_remote_throttling",
        ),
        # No throttling for S3-to-S3, uses native copy
        pytest.param(
            "s3",
            next_backup_name("remote"),
            "server",
            "max_backup_bandwidth_for_server",
            "2M",
            0,
            id="server_remote_to_remote_throttling",
        ),
        #
        # Local -> Remote
        #
        # NOTE: S3 is complex, it will read file 3 times:
        # - first for calculating the checksum
        # - second for calculating the signature
        # - and finally to write the payload to S3
        # Hence the value should be multipled by 3.
        #
        # BUT: only in case of HTTP, HTTPS will not require this.
        pytest.param(
            "default",
            next_backup_name("remote"),
            None,
            None,
            None,
            0,
            id="no_local_to_remote_throttling",
        ),
        # reading 1e6*8 bytes with 1M default bandwidth should take (8-1)/1=7 seconds
        pytest.param(
            "default",
            next_backup_name("remote"),
            "user",
            "max_backup_bandwidth",
            "1M",
            7,
            id="user_local_to_remote_throttling",
        ),
        # reading 1e6*8 bytes with 2M default bandwidth should take (8-2)/2=3 seconds
        pytest.param(
            "default",
            next_backup_name("remote"),
            "server",
            "max_backup_bandwidth_for_server",
            "2M",
            3,
            id="server_local_to_remote_throttling",
        ),
    ],
)
def test_backup_throttling(policy, backup_name, mode, setting, value, should_take):
    node_update_config(mode, setting, value)
    node.query(
        f"""
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='{policy}';
        insert into data select * from numbers(1e6);
    """
    )
    _, took = elapsed(node.query, f"backup table data to {backup_name}")
    assert_took(took, should_take)


def test_backup_throttling_override():
    node_update_config("user", "max_backup_bandwidth", "1M")
    node.query(
        """
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9;
        insert into data select * from numbers(1e6);
    """
    )

    backup_name = next_backup_name("local")
    _, took = elapsed(
        node.query,
        f"backup table data to {backup_name}",
        settings={
            "max_backup_bandwidth": "500K",
        },
    )
    # reading 1e6*8 bytes with 500Ki default bandwidth should take (8-0.5)/0.5=15 seconds
    assert_took(took, 15)


@pytest.mark.parametrize(
    "policy,mode,setting,value,should_take",
    [
        #
        # Local
        #
        pytest.param("default", None, None, None, 0, id="no_local_throttling"),
        # reading 1e6*8 bytes with 1M default bandwidth should take (8-1)/1=7 seconds
        pytest.param(
            "default",
            "user",
            "max_local_read_bandwidth",
            "1M",
            7,
            id="user_local_throttling",
        ),
        # reading 1e6*8 bytes with 2M default bandwidth should take (8-2)/2=3 seconds
        pytest.param(
            "default",
            "server",
            "max_local_read_bandwidth_for_server",
            "2M",
            3,
            id="server_local_throttling",
        ),
        #
        # Remote
        #
        pytest.param("s3", None, None, None, 0, id="no_remote_throttling"),
        # reading 1e6*8 bytes with 1M default bandwidth should take (8-1)/1=7 seconds
        pytest.param(
            "s3",
            "user",
            "max_remote_read_network_bandwidth",
            "1M",
            7,
            id="user_remote_throttling",
        ),
        # reading 1e6*8 bytes with 2M default bandwidth should take (8-2)/2=3 seconds
        pytest.param(
            "s3",
            "server",
            "max_remote_read_network_bandwidth_for_server",
            "2M",
            3,
            id="server_remote_throttling",
        ),
    ],
)
def test_read_throttling(policy, mode, setting, value, should_take):
    node_update_config(mode, setting, value)
    node.query(
        f"""
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='{policy}';
        insert into data select * from numbers(1e6);
    """
    )
    _, took = elapsed(node.query, f"select * from data")
    assert_took(took, should_take)


@pytest.mark.parametrize(
    "policy,mode,setting,value,should_take",
    [
        #
        # Local
        #
        pytest.param("default", None, None, None, 0, id="no_local_throttling"),
        # reading 1e6*8 bytes with 1M default bandwidth should take (8-1)/1=7 seconds
        pytest.param(
            "default",
            "user",
            "max_local_write_bandwidth",
            "1M",
            7,
            id="local_user_throttling",
        ),
        # reading 1e6*8 bytes with 2M default bandwidth should take (8-2)/2=3 seconds
        pytest.param(
            "default",
            "server",
            "max_local_write_bandwidth_for_server",
            "2M",
            3,
            id="local_server_throttling",
        ),
        #
        # Remote
        #
        pytest.param("s3", None, None, None, 0, id="no_remote_throttling"),
        # writing 1e6*8 bytes with 1M default bandwidth should take (8-1)/1=7 seconds
        pytest.param(
            "s3",
            "user",
            "max_remote_write_network_bandwidth",
            "1M",
            7,
            id="user_remote_throttling",
        ),
        # writing 1e6*8 bytes with 2M default bandwidth should take (8-2)/2=3 seconds
        pytest.param(
            "s3",
            "server",
            "max_remote_write_network_bandwidth_for_server",
            "2M",
            3,
            id="server_remote_throttling",
        ),
    ],
)
def test_write_throttling(policy, mode, setting, value, should_take):
    node_update_config(mode, setting, value)
    node.query(
        f"""
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9, storage_policy='{policy}';
    """
    )
    _, took = elapsed(node.query, f"insert into data select * from numbers(1e6)")
    assert_took(took, should_take)


def test_max_mutations_bandwidth_for_server():
    node.query(
        """
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9;
    """
    )
    node.query("insert into data select * from numbers(1e6)")
    _, took = elapsed(
        node.query,
        "alter table data update key = -key where 1 settings mutations_sync = 1",
    )
    # reading 1e6*8 bytes with 1M/s bandwidth should take (8-1)/1=7 seconds
    assert_took(took, 7)


def test_max_merges_bandwidth_for_server():
    node.query(
        """
        drop table if exists data;
        create table data (key UInt64 CODEC(NONE)) engine=MergeTree() order by tuple() settings min_bytes_for_wide_part=1e9;
    """
    )
    node.query("insert into data select * from numbers(1e6)")
    _, took = elapsed(node.query, "optimize table data final")
    # reading 1e6*8 bytes with 1M/s bandwidth should take (8-1)/1=7 seconds
    assert_took(took, 7)
