import uuid

import pytest
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/overrides.yaml"],
    with_minio=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_local_fs_threadpool_reader(start_cluster):
    init_script = """
        drop table if exists default.test_local;

        create table default.test_local
        (key UInt32, value1 UInt32, value2 UInt32)
        engine = MergeTree
        order by key;

        insert into default.test_local
        select number, number + 1, number + 2
        from numbers(2000000);

        optimize table default.test_local final;
    """

    for stmt in [s.strip() for s in init_script.split(";") if s.strip() != ""]:
        node.query(stmt)

    query_id = uuid.uuid4().hex

    node.query(
        sql="""
            select * from default.test_local
            format Null
            settings
                local_filesystem_read_method = 'pread_threadpool',
                min_bytes_to_use_direct_io = 1,
                max_threads = 8,
                log_query_threads = 1;
        """,
        query_id=query_id,
    )
    # Make sure query scope is destroyed
    time.sleep(2)

    node.query("system flush logs query_thread_log")

    threads_used = node.query(f"""
        select uniqExact(thread_id)
        from system.query_thread_log
        where query_id = '{query_id}'
          and thread_name = 'ThreadPoolRead'
    """)

    # For local FS one of the pool threads may be busy with background tasks
    assert 0 < int(threads_used) and int(threads_used) <= 2

    node.query("drop table if exists default.test_local")


def test_remote_fs_threadpool_reader(start_cluster):
    init_script = """
        drop table if exists default.test_remote;

        create table default.test_remote
        (key UInt32, value1 UInt32, value2 UInt32)
        engine = MergeTree
        order by key
        settings storage_policy = 's3_policy';

        insert into default.test_remote
        select number, number + 1, number + 2
        from numbers(2000000);

        optimize table default.test_remote final;
    """

    for stmt in [s.strip() for s in init_script.split(";") if s.strip() != ""]:
        node.query(stmt)

    query_id = uuid.uuid4().hex

    node.query(
        sql="""
            select * from default.test_remote
            format Null
            settings
                remote_filesystem_read_method = 'threadpool',
                max_threads = 8,
                log_query_threads = 1;
        """,
        query_id=query_id,
    )
    # Make sure query scope is destroyed
    time.sleep(2)

    node.query("system flush logs query_thread_log")

    threads_used = node.query(f"""
        select uniqExact(thread_id)
        from system.query_thread_log
        where query_id = '{query_id}'
          and thread_name = 'VFSRead'
    """)

    assert int(threads_used) == 2

    node.query("drop table if exists default.test_remote")
