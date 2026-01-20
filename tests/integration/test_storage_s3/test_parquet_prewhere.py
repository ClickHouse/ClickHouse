import pytest
import os
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import prepare_s3_bucket, S3Uploader
from helpers.iceberg_utils import get_uuid_str
from helpers.config_cluster import minio_secret_key


def generate_data_complex(start, end, div):
    a_values = []
    b_values = []
    c_values = []
    
    for i in range(start, end):
        a_values.append(i // div)

    for i in range(start + 1, end + 1):
        b_values.append(str(i // div))

    for i in range(start + end, end + end):
        c_values.append(i // div)

    return {
        "a": a_values,
        "b": b_values,
        "c": c_values
    }


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            with_minio=True,
            main_configs=[
                "configs/query_log.xml",
                "configs/defaultS3.xml",
            ],
            user_configs=["configs/users.xml"],
        )

        cluster.start()
        prepare_s3_bucket(cluster)
        cluster.default_s3_uploader = S3Uploader(
            cluster.minio_client, cluster.minio_bucket
        )

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("storage_type", ["s3"])
def test_multistage_prewhere(started_cluster, storage_type):
    instance = started_cluster.instances["node1"]
    TABLE_NAME = (
        "test_parquet_prewhere_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    data_dict = generate_data_complex(0, 1000, 100)
    
    schema = pa.schema([
        pa.field("a", pa.int32()),
        pa.field("b", pa.string()),
        pa.field("c", pa.int32()),
    ])

    table = pa.Table.from_pydict(data_dict, schema=schema)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        local_parquet_file = os.path.join(temp_dir, f"{TABLE_NAME}.parquet")
        
        pq.write_table(
            table,
            local_parquet_file,
            row_group_size=1000000000,
            use_dictionary=False
        )
        
        s3_uploader = S3Uploader(started_cluster.minio_client, started_cluster.minio_bucket, use_relpath=False)
        s3_uploader.upload_file(local_parquet_file, f"{TABLE_NAME}.parquet")

    url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{TABLE_NAME}.parquet"
    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME} (a Int32, b String, c Int32) 
        ENGINE = S3('{url}', format = 'Parquet', access_key_id = 'minio', secret_access_key = '{minio_secret_key}')
        """
    )

    count = instance.query(f"SELECT count() FROM {TABLE_NAME}")
    assert int(count) == 1000, f"Expected 1000 rows, got {count}"

    query_id = get_uuid_str()
    target_0 = ""
    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a = 0 AND a = 1") == target_0
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a = 0 AND a = 1", query_id=query_id) == target_0
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '0\t0\n'

    query_id = get_uuid_str()
    target_1 = ""
    for i in range(100):
        target_1 += f"0\t{(i + 1) // 100}\t{(i + 1000) // 100}\n"
    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a = 0") == target_1
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a = 0", query_id=query_id) == target_1
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '1000\t1\n'

    query_id = get_uuid_str()
    target_2 = ""
    for i in range(100, 1000):
        target_2 += f"{i // 100}\t{(i + 1) // 100}\t{(i + 1000) // 100}\n"
    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a != 0") == target_2
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a != 0", query_id=query_id) == target_2
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '1000\t1\n'

    query_id = get_uuid_str()
    target_3 = ""
    for i in range(100, 1000):
        if (i + 1000) // 100 == 13:
            continue
        target_3 += f"{i // 100}\t{(i + 1) // 100}\t{(i + 1000) // 100}\n"
    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a != 0 AND c != 13") == target_3
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a != 0 AND c != 13", query_id=query_id) == target_3
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '1900\t2\n'

    query_id = get_uuid_str()
    target_4 = ""
    for i in range(100, 1000):
        c_val = (i + 1000) // 100
        if c_val >= 12 and c_val < 14:
            target_4 += f"{i // 100}\t{(i + 1) // 100}\t{(i + 1000) // 100}\n"

    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a != 0 AND c >= 12 AND c < 14") == target_4
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a != 0 AND c >= 12 AND c < 14", query_id=query_id) == target_4
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '1900\t2\n'

    query_id = get_uuid_str()
    target_5 = ""
    for i in range(100, 1000):
        c_val = (i + 1000) // 100
        b_val = (i + 1) // 100
        if c_val != 13 and b_val != 12:
            target_5 += f"{i // 100}\t{(i + 1) // 100}\t{(i + 1000) // 100}\n"

    assert instance.query(f"SELECT * FROM {TABLE_NAME} PREWHERE a != 0 AND c != 13 AND b != '12'") == target_5
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a != 0 AND c != 13 AND b != '12'", query_id=query_id) == target_5
    instance.query("SYSTEM FLUSH LOGS query_log;")
    prof_events = instance.query(f"SELECT ProfileEvents['ParquetRowsFilterExpression'], ProfileEvents['ParquetColumnsFilterExpression'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'")
    assert prof_events == '2700\t3\n'
