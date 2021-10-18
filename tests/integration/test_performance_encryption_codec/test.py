import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV
import time
import logging
import statistics


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["configs/encryption_codec.xml"])

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
    codec_clause = (" Codec(LZ4, " + enctype + ")") if enctype else ""
    node.query(f"DROP TABLE IF EXISTS {table_name} NO DELAY")
    node.query(
        """
        CREATE TABLE {} (x Int32{})
        ENGINE=MergeTree() ORDER BY x
        """.format(table_name, codec_clause))

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
    all_enc_types = ["", "AES_128_GCM_SIV", "AES_256_GCM_SIV"]

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
# INSERT: median=0.1912320852279663 seconds, min=0.1806344985961914 seconds
# SELECT: median=0.06103801727294922 seconds, min=0.059705495834350586 seconds
# INSERT (AES_128_GCM_SIV): median=0.22002243995666504 seconds, min=0.21146249771118164, diff=+0.02879035472869873 seconds (+15.055190500264622%)
# SELECT (AES_128_GCM_SIV): median=0.06571781635284424 seconds, min=0.06384015083312988, diff=+0.0046797990798950195 seconds (+7.667023420777151%)
# INSERT (AES_256_GCM_SIV): median=0.22456145286560059 seconds, min=0.21617341041564941, diff=+0.03332936763763428 seconds (+17.428752919732375%)
# SELECT (AES_256_GCM_SIV): median=0.06642782688140869 seconds, min=0.06438946723937988, diff=+0.005389809608459473 seconds (+8.830250144524475%)
