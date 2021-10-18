import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV
import time
import logging
import statistics


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/storage.xml"])

@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        cluster.exec_in_container(node.docker_id, ["mkdir", "-p", "/var/lib/clickhouse/encdsk/"])
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
# INSERT: median=0.19331371784210205 seconds, min=0.18053174018859863 seconds
# SELECT: median=0.06115412712097168 seconds, min=0.059880733489990234 seconds
# INSERT (AES_128_CTR): median=0.20322048664093018 seconds, min=0.19486641883850098, diff=+0.009906768798828125 seconds (+5.1247107082798635%)
# SELECT (AES_128_CTR): median=0.06926953792572021 seconds, min=0.06745171546936035, diff=+0.008115410804748535 seconds (+13.2704221069088%)
# INSERT (AES_192_CTR): median=0.20535707473754883 seconds, min=0.19739723205566406, diff=+0.012043356895446777 seconds (+6.229954619818417%)
# SELECT (AES_192_CTR): median=0.06943714618682861 seconds, min=0.0680079460144043, diff=+0.008283019065856934 seconds (+13.544497249501946%)
# INSERT (AES_256_CTR): median=0.20740187168121338 seconds, min=0.1996595859527588, diff=+0.014088153839111328 seconds (+7.2877155311960236%)
# SELECT (AES_256_CTR): median=0.06984663009643555 seconds, min=0.06796693801879883, diff=+0.008692502975463867 seconds (+14.214090503276816%)
