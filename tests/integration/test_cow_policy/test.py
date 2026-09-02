import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["configs/overrides.yaml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("storage_policy", ["cow_policy_multi_disk", "cow_policy_multi_volume"])
def test_cow_policy(start_cluster, storage_policy):
    try:
        node.query_with_retry(
            f"""
            ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
            (
                price UInt32,
                date Date,
                postcode1 LowCardinality(String),
                postcode2 LowCardinality(String),
                type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
                is_new UInt8,
                duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
                addr1 String,
                addr2 String,
                street LowCardinality(String),
                locality LowCardinality(String),
                town LowCardinality(String),
                district LowCardinality(String),
                county LowCardinality(String)
            )
            ENGINE = MergeTree
            ORDER BY (postcode1, postcode2, addr1, addr2)
            SETTINGS storage_policy = '{storage_policy}'
            """
        )
        prev_count = int(node.query("SELECT count() FROM uk_price_paid"))
        assert prev_count > 0
        for _ in range(0, 10):
            node.query("INSERT INTO uk_price_paid (price) VALUES (1000)")
        new_count = int(node.query("SELECT count() FROM uk_price_paid"))
        assert new_count == prev_count + 10
    finally:
        node.query("DROP TABLE IF EXISTS uk_price_paid SYNC")
