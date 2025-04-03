import io
import json
import logging
import random
import string
import time
import uuid
from multiprocessing.dummy import Pool
import pytest
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.config_cluster import minio_secret_key

DEFAULT_AUTH = ["'minio'", f"'{minio_secret_key}'"]
NO_AUTH = ["NOSIGN"]


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


def random_str(length=6):
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.SystemRandom().choice(alphabet) for _ in range(length))


def generate_random_files(
    started_cluster,
    files_path,
    count,
    storage="s3",
    column_num=3,
    row_num=10,
    start_ind=0,
    bucket=None,
    use_prefix=None,
    use_random_names=False,
    files=None,
):
    if files is not None:
        pass
    elif use_random_names:
        files = [
            (f"{files_path}/{random_str(10)}.csv", i)
            for i in range(start_ind, start_ind + count)
        ]
    elif use_prefix is not None:
        files = [
            (f"{files_path}/{use_prefix}_{i}.csv", i)
            for i in range(start_ind, start_ind + count)
        ]
    else:
        files = [
            (f"{files_path}/test_{i}.csv", i)
            for i in range(start_ind, start_ind + count)
        ]
    files.sort(key=lambda x: x[0])

    print(f"Generating files: {files}")

    total_values = []
    for filename, i in files:
        rand_values = [
            [random.randint(0, 1000) for _ in range(column_num)] for _ in range(row_num)
        ]
        total_values += rand_values
        values_csv = (
            "\n".join((",".join(map(str, row)) for row in rand_values)) + "\n"
        ).encode()
        if storage == "s3":
            put_s3_file_content(started_cluster, filename, values_csv, bucket)
        else:
            put_azure_file_content(started_cluster, filename, values_csv, bucket)
    return total_values


def put_s3_file_content(started_cluster, filename, data, bucket=None):
    bucket = started_cluster.minio_bucket if bucket is None else bucket
    buf = io.BytesIO(data)
    started_cluster.minio_client.put_object(bucket, filename, buf, len(data))


def put_azure_file_content(started_cluster, filename, data, bucket=None):
    client = started_cluster.blob_service_client.get_blob_client(
        started_cluster.azurite_container, filename
    )
    buf = io.BytesIO(data)
    client.upload_blob(buf, "BlockBlob", len(data))


def create_table(
    started_cluster,
    node,
    table_name,
    mode,
    files_path,
    engine_name="S3Queue",
    version=None,
    format="column1 UInt32, column2 UInt32, column3 UInt32",
    additional_settings={},
    file_format="CSV",
    auth=DEFAULT_AUTH,
    bucket=None,
    expect_error=False,
    database_name="default",
    no_settings=False,
):
    auth_params = ",".join(auth)
    bucket = started_cluster.minio_bucket if bucket is None else bucket

    settings = {
        "s3queue_loading_retries": 0,
        "after_processing": "keep",
        "keeper_path": f"/clickhouse/test_{table_name}",
        "mode": f"{mode}",
    }
    if version is None:
        settings["enable_hash_ring_filtering"] = 1

    settings.update(additional_settings)

    engine_def = None
    if engine_name == "S3Queue":
        url = f"http://{started_cluster.minio_host}:{started_cluster.minio_port}/{bucket}/{files_path}/"
        engine_def = f"{engine_name}('{url}', {auth_params}, {file_format})"
    else:
        engine_def = f"{engine_name}('{started_cluster.env_variables['AZURITE_CONNECTION_STRING']}', '{started_cluster.azurite_container}', '{files_path}/', 'CSV')"

    node.query(f"DROP TABLE IF EXISTS {table_name}")
    if no_settings:
        create_query = f"""
            CREATE TABLE {database_name}.{table_name} ({format})
            ENGINE = {engine_def}
            """
    else:
        create_query = f"""
            CREATE TABLE {database_name}.{table_name} ({format})
            ENGINE = {engine_def}
            SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
            """

    if expect_error:
        return node.query_and_get_error(create_query)

    node.query(create_query)


def create_mv(
    node,
    src_table_name,
    dst_table_name,
    mv_name=None,
    create_dst_table_first=True,
    format="column1 UInt32, column2 UInt32, column3 UInt32",
):
    if mv_name is None:
        mv_name = f"{src_table_name}_mv"

    node.query(f"""
        DROP TABLE IF EXISTS {dst_table_name};
        DROP TABLE IF EXISTS {mv_name};
    """)

    if create_dst_table_first:
        node.query(
            f"""
            CREATE TABLE {dst_table_name} ({format}, _path String)
            ENGINE = MergeTree()
            ORDER BY column1;
            CREATE MATERIALIZED VIEW {mv_name} TO {dst_table_name} AS SELECT *, _path FROM {src_table_name};
            """
        )
    else:
        node.query(
            f"""
            SET allow_materialized_view_with_bad_select=1;
            CREATE MATERIALIZED VIEW {mv_name} TO {dst_table_name} AS SELECT *, _path FROM {src_table_name};
            CREATE TABLE {dst_table_name} ({format}, _path String)
            ENGINE = MergeTree()
            ORDER BY column1;
            """
    )


def generate_random_string(length=6):
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))
