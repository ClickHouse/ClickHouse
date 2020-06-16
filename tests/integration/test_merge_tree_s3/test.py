import logging
import random
import string
import time

import pytest
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node", config_dir="configs", with_minio=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


FILES_OVERHEAD = 1
FILES_OVERHEAD_PER_COLUMN = 2  # Data and mark files
FILES_OVERHEAD_PER_PART_WIDE = FILES_OVERHEAD_PER_COLUMN * 3 + 2 + 6
FILES_OVERHEAD_PER_PART_COMPACT = 10


def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign*(i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def create_table(cluster, table_name, additional_settings=None):
    node = cluster.instances["node"]

    create_table_statement = """
        CREATE TABLE {} (
            dt Date,
            id Int64,
            data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=MergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS
            storage_policy='s3',
            old_parts_lifetime=0, 
            index_granularity=512
        """.format(table_name)

    if additional_settings:
        create_table_statement += ","
        create_table_statement += additional_settings

    node.query(create_table_statement)


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    node = cluster.instances["node"]
    minio = cluster.minio_client

    node.query("DROP TABLE IF EXISTS s3_test NO DELAY")
    time.sleep(1)
    try:
        assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == 0
    finally:
        # Remove extra objects to prevent tests cascade failing
        for obj in list(minio.list_objects(cluster.minio_bucket, 'data/')):
            minio.remove_object(cluster.minio_bucket, obj.object_name)


@pytest.mark.parametrize(
    "min_rows_for_wide_part,files_per_part",
    [
        (0, FILES_OVERHEAD_PER_PART_WIDE),
        (8192, FILES_OVERHEAD_PER_PART_COMPACT)
    ]
)
def test_simple_insert_select(cluster, min_rows_for_wide_part, files_per_part):
    create_table(cluster, "s3_test", additional_settings="min_rows_for_wide_part={}".format(min_rows_for_wide_part))

    node = cluster.instances["node"]
    minio = cluster.minio_client

    values1 = generate_values('2020-01-03', 4096)
    node.query("INSERT INTO s3_test VALUES {}".format(values1))
    assert node.query("SELECT * FROM s3_test order by dt, id FORMAT Values") == values1
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + files_per_part

    values2 = generate_values('2020-01-04', 4096)
    node.query("INSERT INTO s3_test VALUES {}".format(values2))
    assert node.query("SELECT * FROM s3_test ORDER BY dt, id FORMAT Values") == values1 + "," + values2
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + files_per_part*2

    assert node.query("SELECT count(*) FROM s3_test where id = 1 FORMAT Values") == "(2)"


@pytest.mark.parametrize(
    "merge_vertical", [False, True]
)
def test_insert_same_partition_and_merge(cluster, merge_vertical):
    settings = None
    if merge_vertical:
        settings = """
            vertical_merge_algorithm_min_rows_to_activate=0,
            vertical_merge_algorithm_min_columns_to_activate=0
        """
    create_table(cluster, "s3_test", additional_settings=settings)

    node = cluster.instances["node"]
    minio = cluster.minio_client

    node.query("SYSTEM STOP MERGES s3_test")
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 1024)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 2048)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 4096)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 1024, -1)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 2048, -1)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 4096, -1)))
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(distinct(id)) FROM s3_test FORMAT Values") == "(8192)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD_PER_PART_WIDE*6 + FILES_OVERHEAD

    node.query("SYSTEM START MERGES s3_test")
    # Wait for merges and old parts deletion
    time.sleep(3)

    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(distinct(id)) FROM s3_test FORMAT Values") == "(8192)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD


def test_alter_table_columns(cluster):
    create_table(cluster, "s3_test")

    node = cluster.instances["node"]
    minio = cluster.minio_client

    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 4096)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 4096, -1)))

    node.query("ALTER TABLE s3_test ADD COLUMN col1 UInt64 DEFAULT 1")
    # To ensure parts have merged
    node.query("OPTIMIZE TABLE s3_test")

    # Wait for merges, mutations and old parts deletion
    time.sleep(3)

    assert node.query("SELECT sum(col1) FROM s3_test FORMAT Values") == "(8192)"
    assert node.query("SELECT sum(col1) FROM s3_test WHERE id > 0 FORMAT Values") == "(4096)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD_PER_COLUMN

    node.query("ALTER TABLE s3_test MODIFY COLUMN col1 String", settings={"mutations_sync": 2})

    # Wait for old parts deletion
    time.sleep(3)

    assert node.query("SELECT distinct(col1) FROM s3_test FORMAT Values") == "('1')"
    # and file with mutation
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == (FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD_PER_COLUMN + 1)

    node.query("ALTER TABLE s3_test DROP COLUMN col1", settings={"mutations_sync": 2})

    # Wait for old parts deletion
    time.sleep(3)

    # and 2 files with mutations
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + 2


def test_attach_detach_partition(cluster):
    create_table(cluster, "s3_test")

    node = cluster.instances["node"]
    minio = cluster.minio_client

    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 4096)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-04', 4096)))
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE*2

    node.query("ALTER TABLE s3_test DETACH PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(4096)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE*2

    node.query("ALTER TABLE s3_test ATTACH PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE*2

    node.query("ALTER TABLE s3_test DROP PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(4096)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE

    node.query("ALTER TABLE s3_test DETACH PARTITION '2020-01-04'")
    node.query("ALTER TABLE s3_test DROP DETACHED PARTITION '2020-01-04'", settings={"allow_drop_detached": 1})
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(0)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD


def test_move_partition_to_another_disk(cluster):
    create_table(cluster, "s3_test")

    node = cluster.instances["node"]
    minio = cluster.minio_client

    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 4096)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-04', 4096)))
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE*2

    node.query("ALTER TABLE s3_test MOVE PARTITION '2020-01-04' TO DISK 'hdd'")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE


def test_table_manipulations(cluster):
    create_table(cluster, "s3_test")

    node = cluster.instances["node"]
    minio = cluster.minio_client

    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 4096)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-04', 4096)))

    node.query("RENAME TABLE s3_test TO s3_renamed")
    assert node.query("SELECT count(*) FROM s3_renamed FORMAT Values") == "(8192)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE*2
    node.query("RENAME TABLE s3_renamed TO s3_test")

    assert node.query("CHECK TABLE s3_test FORMAT Values") == "(1)"

    node.query("DETACH TABLE s3_test")
    node.query("ATTACH TABLE s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE*2

    node.query("TRUNCATE TABLE s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(0)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD


def test_move_replace_partition_to_another_table(cluster):
    create_table(cluster, "s3_test")

    node = cluster.instances["node"]
    minio = cluster.minio_client

    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 4096)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-04', 4096)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-05', 4096, -1)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-06', 4096, -1)))
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(16384)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE*4

    create_table(cluster, "s3_clone")

    node.query("ALTER TABLE s3_test MOVE PARTITION '2020-01-03' TO TABLE s3_clone")
    node.query("ALTER TABLE s3_test MOVE PARTITION '2020-01-05' TO TABLE s3_clone")
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert node.query("SELECT sum(id) FROM s3_clone FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_clone FORMAT Values") == "(8192)"
    # Number of objects in S3 should be unchanged.
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD*2 + FILES_OVERHEAD_PER_PART_WIDE*4

    # Add new partitions to source table, but with different values and replace them from copied table.
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 4096, -1)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-05', 4096)))
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(16384)"
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD*2 + FILES_OVERHEAD_PER_PART_WIDE*6

    node.query("ALTER TABLE s3_test REPLACE PARTITION '2020-01-03' FROM s3_clone")
    node.query("ALTER TABLE s3_test REPLACE PARTITION '2020-01-05' FROM s3_clone")
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(16384)"
    assert node.query("SELECT sum(id) FROM s3_clone FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_clone FORMAT Values") == "(8192)"

    # Wait for outdated partitions deletion.
    time.sleep(3)
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD*2 + FILES_OVERHEAD_PER_PART_WIDE*4

    node.query("DROP TABLE s3_clone NO DELAY")
    time.sleep(1)
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(16384)"
    # Data should remain in S3
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE*4

    node.query("ALTER TABLE s3_test FREEZE")
    # Number S3 objects should be unchanged.
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE*4

    node.query("DROP TABLE s3_test NO DELAY")
    time.sleep(1)
    # Backup data should remain in S3.
    assert len(list(minio.list_objects(cluster.minio_bucket, 'data/'))) == FILES_OVERHEAD_PER_PART_WIDE*4

    for obj in list(minio.list_objects(cluster.minio_bucket, 'data/')):
        minio.remove_object(cluster.minio_bucket, obj.object_name)
