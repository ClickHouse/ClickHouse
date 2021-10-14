import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV
import time
import logging
import statistics


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node",
                            main_configs=["configs/storage.xml"],
                            tmpfs=["/disk:size=3G"])

@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def get_table_name(enctype):
    return "tbl" + (("_" + enctype) if enctype else "")

def create_table(enctype):
    table_name = get_table_name(enctype)
    if not enctype:
        policy_name = "local_policy"
    elif enctype == "AES_128_CTR":
        policy_name = "policy_aes_128_ctr"
    elif enctype == "AES_192_CTR":
        policy_name = "policy_aes_192_ctr"
    elif enctype == "AES_256_CTR":
        policy_name = "policy_aes_256_ctr"
    node.query(f"DROP TABLE IF EXISTS {table_name} NO DELAY")
    node.query(
        """
        CREATE TABLE {} (x Int32)
        ENGINE=MergeTree() ORDER BY x
        SETTINGS storage_policy='{}'
        """.format(table_name, policy_name))

def insert_data(enctype, count):
    table_name = get_table_name(enctype)
    node.query(f"INSERT INTO {table_name} SELECT number AS x FROM numbers({count})")

def read_data(enctype, count):
    table_name = get_table_name(enctype)
    expected = count * (count - 1) / 2
    assert node.query(f"SELECT sum(x) FROM {table_name}") == TSV([[int(expected)]])

def truncate_table(enctype):
    table_name = get_table_name(enctype)
    node.query(f"TRUNCATE TABLE {table_name}")

# Actual test:

def test_performance(capsys):
    count = 10000000
    num_repeats = 200
    all_enc_types = ["", "AES_128_CTR", "AES_192_CTR", "AES_256_CTR"]

    for enctype in all_enc_types:
        create_table(enctype)
    
    all_insert_times = {}
    all_read_times = {}
    for rep in range(num_repeats):
        for enctype in all_enc_types:
            start_time = time.time()
            insert_data(enctype, count)
            elapsed = time.time() - start_time
            if enctype in all_insert_times:
                all_insert_times[enctype].append(elapsed)
            else:
                all_insert_times[enctype] = [elapsed]

            start_time = time.time()
            read_data(enctype, count)
            elapsed = time.time() - start_time
            if enctype in all_read_times:
                all_read_times[enctype].append(elapsed)
            else:
                all_read_times[enctype] = [elapsed]

            truncate_table(enctype)

    avg_insert_times = {}
    avg_read_times = {}
    min_insert_times = {}
    min_read_times = {}
    for enctype in all_enc_types:
        avg_insert_times[enctype] = statistics.median(all_insert_times[enctype])
        avg_read_times[enctype] = statistics.median(all_read_times[enctype])
        min_insert_times[enctype] = min(all_insert_times[enctype])
        min_read_times[enctype] = min(all_read_times[enctype])

    with capsys.disabled():
        print("\n")
        for enctype in all_enc_types:
            insert_time = avg_insert_times[enctype]
            read_time = avg_read_times[enctype]
            min_insert_time = min_insert_times[enctype]
            min_read_time = min_read_times[enctype]
            if not enctype:
                print(f"INSERT: median={insert_time} seconds, min={min_insert_time} seconds")
                print(f"SELECT: median={read_time} seconds, min={min_read_time} seconds")
                continue
            insert_time_0 = avg_insert_times[""]
            insert_time_diff = insert_time - insert_time_0
            insert_time_ratio = insert_time_diff / insert_time_0
            read_time_0 = avg_read_times[""]
            read_time_diff = read_time - read_time_0
            read_time_ratio = read_time_diff / read_time_0
            print(f"INSERT ({enctype}): median={insert_time} seconds, min={min_insert_time}, diff=+{insert_time_diff} seconds (+{insert_time_ratio * 100}%)")
            print(f"SELECT ({enctype}): median={read_time} seconds, min={min_read_time}, diff=+{read_time_diff} seconds (+{read_time_ratio * 100}%)")


# count = 10000000
# num_repeats = 200
# INSERT: median=0.17236435413360596 seconds, min=0.16443753242492676 seconds
# SELECT: median=0.06171703338623047 seconds, min=0.05977463722229004 seconds
# INSERT (AES_128_CTR): median=0.1868298053741455 seconds, min=0.1780850887298584, diff=+0.01446545124053955 seconds (+8.392368197734694%)
# SELECT (AES_128_CTR): median=0.06535756587982178 seconds, min=0.06269025802612305, diff=+0.0036405324935913086 seconds (+5.898748358185892%)
# INSERT (AES_192_CTR): median=0.18871784210205078 seconds, min=0.18044328689575195, diff=+0.016353487968444824 seconds (+9.48774359446074%)
# SELECT (AES_192_CTR): median=0.0659334659576416 seconds, min=0.06291937828063965, diff=+0.004216432571411133 seconds (+6.831878235339565%)
# INSERT (AES_256_CTR): median=0.19133424758911133 seconds, min=0.18201780319213867, diff=+0.01896989345550537 seconds (+11.005694043213312%)
# SELECT (AES_256_CTR): median=0.06623411178588867 seconds, min=0.06383156776428223, diff=+0.004517078399658203 seconds (+7.319014138916789%)
