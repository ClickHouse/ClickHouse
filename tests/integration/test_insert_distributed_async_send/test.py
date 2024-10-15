# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

# NOTES:
# - timeout should not be reduced due to bit flip of the corrupted buffer

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# n1 -- distributed_background_insert_batch=1
n1 = cluster.add_instance(
    "n1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.d/batch.xml"],
)
# n2 -- distributed_background_insert_batch=0
n2 = cluster.add_instance(
    "n2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.d/no_batch.xml"],
)

# n3 -- distributed_background_insert_batch=1/distributed_background_insert_split_batch_on_failure=1
n3 = cluster.add_instance(
    "n3",
    main_configs=["configs/remote_servers_split.xml"],
    user_configs=[
        "configs/users.d/batch.xml",
        "configs/users.d/split.xml",
    ],
)
# n4 -- distributed_background_insert_batch=0/distributed_background_insert_split_batch_on_failure=1
n4 = cluster.add_instance(
    "n4",
    main_configs=["configs/remote_servers_split.xml"],
    user_configs=[
        "configs/users.d/no_batch.xml",
        "configs/users.d/split.xml",
    ],
)

batch_params = pytest.mark.parametrize(
    "batch",
    [
        (1),
        (0),
    ],
)

batch_and_split_params = pytest.mark.parametrize(
    "batch,split",
    [
        (1, 0),
        (0, 0),
        (1, 1),
        (0, 1),
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(remote_cluster_name):
    for _, instance in list(cluster.instances.items()):
        instance.query(
            "CREATE TABLE data (key Int, value String) Engine=MergeTree() ORDER BY key"
        )
        instance.query(
            f"""
        CREATE TABLE dist AS data
        Engine=Distributed(
            {remote_cluster_name},
            currentDatabase(),
            data,
            key
        )
        """
        )
        # only via SYSTEM FLUSH DISTRIBUTED
        instance.query("SYSTEM STOP DISTRIBUTED SENDS dist")


def drop_tables():
    for _, instance in list(cluster.instances.items()):
        instance.query("DROP TABLE IF EXISTS data")
        instance.query("DROP TABLE IF EXISTS dist")


# return amount of bytes of the 2.bin for n2 shard
def insert_data(node):
    node.query(
        "INSERT INTO dist SELECT number, randomPrintableASCII(100) FROM numbers(10000)",
        settings={
            # do not do direct INSERT, always via SYSTEM FLUSH DISTRIBUTED
            "prefer_localhost_replica": 0,
        },
    )
    path = get_path_to_dist_batch()
    size = int(node.exec_in_container(["bash", "-c", f"wc -c < {path}"]))
    assert size > 1 << 16
    return size


def get_node(batch, split=None):
    if split:
        if batch:
            return n3
        return n4
    if batch:
        return n1
    return n2


def bootstrap(batch, split=None):
    drop_tables()
    create_tables("insert_distributed_async_send_cluster_two_replicas")
    return insert_data(get_node(batch, split))


def get_path_to_dist_batch(file="2.bin"):
    # There are:
    # - /var/lib/clickhouse/data/default/dist/shard1_replica1/1.bin
    # - /var/lib/clickhouse/data/default/dist/shard1_replica2/2.bin
    #
    # @return the file for the n2 shard
    return f"/var/lib/clickhouse/data/default/dist/shard1_replica2/{file}"


def check_dist_after_corruption(truncate, batch, split=None):
    node = get_node(batch, split)

    if batch:
        # In batch mode errors are ignored
        node.query("SYSTEM FLUSH DISTRIBUTED dist")
    else:
        if truncate:
            with pytest.raises(
                QueryRuntimeException, match="Cannot read all data. Bytes read:"
            ):
                node.query("SYSTEM FLUSH DISTRIBUTED dist")
        else:
            with pytest.raises(
                QueryRuntimeException,
                match="Checksum doesn't match: corrupted data. Reference:",
            ):
                node.query("SYSTEM FLUSH DISTRIBUTED dist")

    # send pending files
    # (since we have two nodes and corrupt file for only one of them)
    node.query("SYSTEM FLUSH DISTRIBUTED dist")

    # but there is broken file
    broken = get_path_to_dist_batch("broken")
    node.exec_in_container(["bash", "-c", f"ls {broken}/2.bin"])

    if split:
        assert int(n3.query("SELECT count() FROM data")) == 10000
        assert int(n4.query("SELECT count() FROM data")) == 0
    else:
        assert int(n1.query("SELECT count() FROM data")) == 10000
        assert int(n2.query("SELECT count() FROM data")) == 0


@batch_params
def test_insert_distributed_async_send_success(batch):
    bootstrap(batch)
    node = get_node(batch)
    node.query("SYSTEM FLUSH DISTRIBUTED dist")
    assert int(n1.query("SELECT count() FROM data")) == 10000
    assert int(n2.query("SELECT count() FROM data")) == 10000


@batch_and_split_params
def test_insert_distributed_async_send_truncated_1(batch, split):
    size = bootstrap(batch, split)
    path = get_path_to_dist_batch()
    node = get_node(batch, split)

    new_size = size - 10
    # we cannot use truncate, due to hardlinks
    node.exec_in_container(
        ["bash", "-c", f"mv {path} /tmp/bin && head -c {new_size} /tmp/bin > {path}"]
    )

    check_dist_after_corruption(True, batch, split)


@batch_params
def test_insert_distributed_async_send_truncated_2(batch):
    bootstrap(batch)
    path = get_path_to_dist_batch()
    node = get_node(batch)

    # we cannot use truncate, due to hardlinks
    node.exec_in_container(
        ["bash", "-c", f"mv {path} /tmp/bin && head -c 10000 /tmp/bin > {path}"]
    )

    check_dist_after_corruption(True, batch)


# The difference from the test_insert_distributed_async_send_corrupted_small
# is that small corruption will be seen only on local node
@batch_params
def test_insert_distributed_async_send_corrupted_big(batch):
    size = bootstrap(batch)
    path = get_path_to_dist_batch()

    node = get_node(batch)

    from_original_size = size - 8192
    zeros_size = 8192
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {path} /tmp/bin && head -c {from_original_size} /tmp/bin > {path} && head -c {zeros_size} /dev/zero >> {path}",
        ]
    )

    check_dist_after_corruption(False, batch)


@batch_params
def test_insert_distributed_async_send_corrupted_small(batch):
    size = bootstrap(batch)
    path = get_path_to_dist_batch()
    node = get_node(batch)

    from_original_size = size - 60
    zeros_size = 60
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {path} /tmp/bin && head -c {from_original_size} /tmp/bin > {path} && head -c {zeros_size} /dev/zero >> {path}",
        ]
    )

    check_dist_after_corruption(False, batch)


@batch_params
def test_insert_distributed_async_send_different_header(batch):
    """
    Check INSERT Into Distributed() with different headers in *.bin
    If batching will not distinguish headers underlying table will never receive the data.
    """

    drop_tables()
    create_tables("insert_distributed_async_send_cluster_two_shards")

    node = get_node(batch)
    node.query(
        "INSERT INTO dist VALUES (0, 'f')",
        settings={
            "prefer_localhost_replica": 0,
        },
    )
    node.query("ALTER TABLE dist MODIFY COLUMN value UInt64")
    node.query(
        "INSERT INTO dist VALUES (2, 1)",
        settings={
            "prefer_localhost_replica": 0,
        },
    )

    n1.query(
        "ALTER TABLE data MODIFY COLUMN value UInt64",
        settings={
            "mutations_sync": 1,
        },
    )

    if batch:
        # but only one batch will be sent, and first is with UInt64 column, so
        # one rows inserted, and for string ('f') exception will be throw.
        with pytest.raises(
            QueryRuntimeException,
            match=r"DB::Exception: Cannot parse string 'f' as UInt64: syntax error at begin of string",
        ):
            node.query("SYSTEM FLUSH DISTRIBUTED dist")
        assert int(n1.query("SELECT count() FROM data")) == 1
        # but once underlying column String, implicit conversion will do the
        # thing, and insert left batch.
        n1.query(
            """
        DROP TABLE data SYNC;
        CREATE TABLE data (key Int, value String) Engine=MergeTree() ORDER BY key;
        """
        )
        node.query("SYSTEM FLUSH DISTRIBUTED dist")
        assert int(n1.query("SELECT count() FROM data")) == 1
    else:
        # first send with String ('f'), so zero rows will be inserted
        with pytest.raises(
            QueryRuntimeException,
            match=r"DB::Exception: Cannot parse string 'f' as UInt64: syntax error at begin of string",
        ):
            node.query("SYSTEM FLUSH DISTRIBUTED dist")
        assert int(n1.query("SELECT count() FROM data")) == 0
        # but once underlying column String, implicit conversion will do the
        # thing, and insert 2 rows (mixed UInt64 and String).
        n1.query(
            """
        DROP TABLE data SYNC;
        CREATE TABLE data (key Int, value String) Engine=MergeTree() ORDER BY key;
        """
        )
        node.query("SYSTEM FLUSH DISTRIBUTED dist")
        assert int(n1.query("SELECT count() FROM data")) == 2

    assert int(n2.query("SELECT count() FROM data")) == 0
