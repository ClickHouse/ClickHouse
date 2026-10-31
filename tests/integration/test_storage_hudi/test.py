import io
import logging
import os
import re
import shutil
import tempfile
import threading
import uuid
from datetime import datetime

import pyspark
import pytest
from pyspark.sql.functions import (
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.s3_tools import (
    prepare_s3_bucket,
    remove_directory,
    upload_directory,
)
from helpers.test_tools import TSV
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_spark(log_dir=None):
    builder = (
        pyspark.sql.SparkSession.builder.appName("test_storage_hudi")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.sql.catalog.local", "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
        )
        .config("spark.driver.memory", "20g")
        .master("local")
    )

    if log_dir:
        props_path = write_spark_log_config(log_dir)
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile=file:{props_path}",
        )

    return builder.getOrCreate()


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__, with_spark=True)
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/named_collections.xml"],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        cluster.spark_session = ResilientSparkSession(
            lambda: get_spark(cluster.instances_dir)
        )

        yield cluster

    finally:
        cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


def write_hudi_from_df(spark, table_name, df, result_path, mode="overwrite"):
    if mode == "overwrite":
        hudi_write_mode = "insert_overwrite"
    else:
        hudi_write_mode = "upsert"

    df.write.mode(mode).option("compression", "none").option(
        "compression", "none"
    ).format("hudi").option("hoodie.table.name", table_name).option(
        "hoodie.datasource.write.partitionpath.field", "partitionpath"
    ).option(
        "hoodie.datasource.write.table.name", table_name
    ).option(
        "hoodie.datasource.write.operation", hudi_write_mode
    ).option(
        "hoodie.datasource.compaction.async.enable", "true"
    ).option(
        "hoodie.compact.inline", "false"
    ).option(
        "hoodie.compact.inline.max.delta.commits", "10"
    ).option(
        "hoodie.parquet.compression.codec", "snappy"
    ).option(
        "hoodie.hfile.compression.algorithm", "uncompressed"
    ).option(
        "hoodie.datasource.write.recordkey.field", "a"
    ).option(
        "hoodie.datasource.write.precombine.field", "a"
    ).save(
        result_path
    )


def write_hudi_from_file(spark, table_name, path, result_path):
    spark.conf.set("spark.sql.debug.maxToStringFields", 100000)
    df = spark.read.load(f"file://{path}")
    write_hudi_from_df(spark, table_name, df, result_path)


def generate_data(spark, start, end, append=1):
    a = spark.range(start, end, 1).toDF("a")
    b = spark.range(start + append, end + append, 1).toDF("b")
    b = b.withColumn("b", b["b"].cast(StringType()))

    a = a.withColumn(
        "row_index", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )
    b = b.withColumn(
        "row_index", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )

    df = a.join(b, on=["row_index"]).drop("row_index")
    return df


def create_hudi_table(node, table_name):
    node.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name}
        ENGINE=Hudi(s3, filename = '{table_name}/')"""
    )


def create_initial_data_file(
    cluster, node, query, table_name, compression_method="none"
):
    node.query(
        f"""
        INSERT INTO TABLE FUNCTION
            file('{table_name}.parquet')
        SETTINGS
            output_format_parquet_compression_method='{compression_method}',
            engine_file_truncate_on_insert=1 {query}
        FORMAT Parquet"""
    )
    user_files_path = os.path.join(
        SCRIPT_DIR, f"{cluster.instances_dir_name}/node1/database/user_files"
    )
    result_path = f"{user_files_path}/{table_name}.parquet"
    return result_path


def test_single_hudi_file(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = f"test_single_hudi_file_{uuid.uuid4().hex[:8]}"

    inserted_data = "SELECT number as a, toString(number) as b FROM numbers(100)"
    parquet_data_path = create_initial_data_file(
        started_cluster, instance, inserted_data, TABLE_NAME
    )

    write_hudi_from_file(spark, TABLE_NAME, parquet_data_path, f"/{TABLE_NAME}")
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 1
    assert files[0].endswith(".parquet")
    create_hudi_table(instance, TABLE_NAME)
    assert instance.query(f"SELECT a, b FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )

    shutil.rmtree(f"/{TABLE_NAME}")
    remove_directory(minio_client, bucket, TABLE_NAME)


def test_multiple_hudi_files(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = f"test_multiple_hudi_files_{uuid.uuid4().hex[:8]}"

    write_hudi_from_df(
        spark, TABLE_NAME, generate_data(spark, 0, 100), f"/{TABLE_NAME}"
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 1

    create_hudi_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_hudi_from_df(
        spark,
        TABLE_NAME,
        generate_data(spark, 100, 200),
        f"/{TABLE_NAME}",
        mode="append",
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 2

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 200
    assert instance.query(
        f"SELECT a, b FROM {TABLE_NAME} ORDER BY 1"
    ) == instance.query("SELECT number, toString(number + 1) FROM numbers(200)")

    write_hudi_from_df(
        spark,
        TABLE_NAME,
        generate_data(spark, 100, 300),
        f"/{TABLE_NAME}",
        mode="append",
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 3

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 300
    assert instance.query(
        f"SELECT a, b FROM {TABLE_NAME} ORDER BY 1"
    ) == instance.query("SELECT number, toString(number + 1) FROM numbers(300)")

    assert int(instance.query(f"SELECT b FROM {TABLE_NAME} WHERE a = 100")) == 101
    write_hudi_from_df(
        spark,
        TABLE_NAME,
        generate_data(spark, 100, 101, append=0),
        f"/{TABLE_NAME}",
        mode="append",
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 300
    assert int(instance.query(f"SELECT b FROM {TABLE_NAME} WHERE a = 100")) == 100

    write_hudi_from_df(
        spark,
        TABLE_NAME,
        generate_data(spark, 100, 1000000, append=0),
        f"/{TABLE_NAME}",
        mode="append",
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 1000000

    shutil.rmtree(f"/{TABLE_NAME}")
    remove_directory(minio_client, bucket, TABLE_NAME)


def test_types(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = f"test_types_{uuid.uuid4().hex[:8]}"

    data = [
        (
            123,
            "string",
            datetime.strptime("2000-01-01", "%Y-%m-%d"),
            ["str1", "str2"],
            True,
        )
    ]
    schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DateType()),
            StructField("d", ArrayType(StringType())),
            StructField("e", BooleanType()),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    write_hudi_from_df(spark, TABLE_NAME, df, f"/{TABLE_NAME}", mode="overwrite")

    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    create_hudi_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 1
    assert (
        instance.query(f"SELECT a, b, c, d, e FROM {TABLE_NAME}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    table_function = f"hudi(s3, filename='{TABLE_NAME}/')"
    assert (
        instance.query(f"SELECT a, b, c, d, e FROM {table_function}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    assert instance.query(f"DESCRIBE {table_function} FORMAT TSV") == TSV(
        [
            ["_hoodie_commit_time", "Nullable(String)"],
            ["_hoodie_commit_seqno", "Nullable(String)"],
            ["_hoodie_record_key", "Nullable(String)"],
            ["_hoodie_partition_path", "Nullable(String)"],
            ["_hoodie_file_name", "Nullable(String)"],
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(String)"],
            ["c", "Nullable(Date32)"],
            ["d", "Array(String)"],
            ["e", "Nullable(Bool)"],
        ]
    )

    shutil.rmtree(f"/{TABLE_NAME}")
    remove_directory(minio_client, bucket, TABLE_NAME)


def write_hudi_table(
    spark, path, rows, operation, table_version, partitioned, extra_options={}
):
    schema = (
        "id long, p string, v long, ts long"
        if partitioned
        else "id long, v long, ts long"
    )
    options = {
        "hoodie.table.name": os.path.basename(path),
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.operation": operation,
        "hoodie.write.table.version": str(table_version),
        "hoodie.metadata.enable": "false",
        "hoodie.datasource.write.partitionpath.field": "p" if partitioned else "",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen."
        + ("SimpleKeyGenerator" if partitioned else "NonpartitionedKeyGenerator"),
        **extra_options,
    }
    mode = "append" if os.path.exists(os.path.join(path, ".hoodie")) else "overwrite"
    spark.createDataFrame(rows, schema).write.format("hudi").options(**options).mode(
        mode
    ).save(path)


def upload_hudi_table(minio_client, bucket, path, prefix):
    # Unlike `upload_directory`, uploads the `.hoodie` timeline too.
    for root, _, files in os.walk(path):
        for name in files:
            if not name.endswith(".crc"):
                file = os.path.join(root, name)
                minio_client.fput_object(
                    bucket, prefix + os.path.relpath(file, path), file
                )


def put_object(minio_client, bucket, key, data):
    minio_client.put_object(bucket, key, io.BytesIO(data), len(data))


def put_hudi_properties(minio_client, bucket, path, prefix, line, new_line):
    with open(os.path.join(path, ".hoodie", "hoodie.properties")) as f:
        lines = f.read().splitlines()
    assert line in lines
    content = "".join((new_line if l == line else l) + "\n" for l in lines)
    put_object(
        minio_client, bucket, prefix + ".hoodie/hoodie.properties", content.encode()
    )


def completed_replace_commits(path, table_version):
    if table_version == 6:
        directory, pattern = ".hoodie", r"\d+\.replacecommit"
    else:
        directory, pattern = ".hoodie/timeline", r"\d+_\d+\.replacecommit"
    names = os.listdir(os.path.join(path, directory))
    return [name for name in names if re.fullmatch(pattern, name)]


def parquet_files(path):
    return {
        os.path.relpath(os.path.join(root, name), path)
        for root, _, files in os.walk(path)
        for name in files
        if name.endswith(".parquet")
    }


def partition_file_ids(path, partition):
    return {
        name.split("_")[0]
        for name in os.listdir(os.path.join(path, partition))
        if name.endswith(".parquet")
    }


def assert_rows(instance, source, columns, expected):
    result = instance.query(f"SELECT {columns} FROM {source} ORDER BY id")
    assert result == "".join("\t".join(map(str, row)) + "\n" for row in expected)
    assert int(instance.query(f"SELECT count() FROM {source}")) == len(expected)


@pytest.mark.parametrize("table_version", [6, 9])
def test_replaced_file_groups(started_cluster, table_version):
    """A completed `replacecommit` (insert_overwrite, insert_overwrite_table,
    clustering) removes file groups whose files stay on storage until the cleaner
    runs. ClickHouse must not read them."""
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    name = f"test_replaced_file_groups_{table_version}_{uuid.uuid4().hex[:8]}"
    root_bucket = f"hudi-root-{uuid.uuid4().hex[:8]}"
    table = f"{name}_engine"
    tmp_dir = tempfile.mkdtemp()

    try:
        # A: a non-partitioned table.
        path = os.path.join(tmp_dir, name)
        prefix = f"{name}/"
        source = f"hudi(s3, filename='{prefix}')"
        write_hudi_table(
            spark,
            path,
            [(i, i * 7, 1) for i in range(30)],
            "insert",
            table_version,
            False,
        )
        with open(os.path.join(path, ".hoodie", "hoodie.properties")) as f:
            properties = f.read().splitlines()
        assert f"hoodie.table.version={table_version}" in properties
        layout = 1 if table_version == 6 else 2
        assert f"hoodie.timeline.layout.version={layout}" in properties
        upload_hudi_table(minio_client, bucket, path, prefix)
        assert_rows(instance, source, "id, v", [(i, i * 7) for i in range(30)])

        # A custom timeline path of a layout 2 table is refused, even without any
        # replacecommit. Layout 1 tables have their timeline in `.hoodie` regardless.
        put_hudi_properties(
            minio_client,
            bucket,
            path,
            prefix,
            "hoodie.timeline.path=timeline",
            "hoodie.timeline.path=../outside_timeline",
        )
        if table_version == 9:
            error = instance.query_and_get_error(f"SELECT count() FROM {source}")
            assert "NOT_IMPLEMENTED" in error
        else:
            assert_rows(instance, source, "id, v", [(i, i * 7) for i in range(30)])
        upload_hudi_table(minio_client, bucket, path, prefix)

        files = parquet_files(path)
        write_hudi_table(
            spark,
            path,
            [(i, i * 100, 2) for i in range(5)],
            "insert_overwrite_table",
            table_version,
            False,
        )
        assert len(completed_replace_commits(path, table_version)) == 1
        assert files <= parquet_files(path)
        upload_hudi_table(minio_client, bucket, path, prefix)
        assert_rows(instance, source, "id, v", [(i, i * 100) for i in range(5)])

        if table_version == 9:
            # D: the same table at the root of a bucket.
            minio_client.make_bucket(root_bucket)
            upload_hudi_table(minio_client, root_bucket, path, "")
            assert_rows(
                instance,
                f"hudi('http://minio1:9001/{root_bucket}/', '{minio_access_key}', '{minio_secret_key}')",
                "id, v",
                [(i, i * 100) for i in range(5)],
            )

        write_hudi_table(
            spark,
            path,
            [(i, i * 7, 3) for i in range(5, 10)],
            "insert",
            table_version,
            False,
        )
        upload_hudi_table(minio_client, bucket, path, prefix)
        expected = [(i, i * 100) for i in range(5)] + [(i, i * 7) for i in range(5, 10)]
        assert_rows(instance, source, "id, v", expected)

        files = parquet_files(path)
        write_hudi_table(
            spark,
            path,
            [(i, i * 7, 4) for i in range(10, 15)],
            "insert",
            table_version,
            False,
            {
                "hoodie.clustering.inline": "true",
                "hoodie.clustering.inline.max.commits": "1",
            },
        )
        assert len(completed_replace_commits(path, table_version)) == 2
        assert files <= parquet_files(path)
        upload_hudi_table(minio_client, bucket, path, prefix)
        expected += [(i, i * 7) for i in range(10, 15)]
        assert_rows(instance, source, "id, v", expected)

        # B: a partitioned table, insert_overwrite of one partition, then of the table.
        path = os.path.join(tmp_dir, f"{name}_partitioned")
        prefix = f"{name}_partitioned/"
        source = f"hudi(s3, filename='{prefix}')"
        rows = [(i, "x/a" if i < 15 else "y/a", i * 7, 1) for i in range(30)]
        write_hudi_table(
            spark,
            path,
            rows,
            "insert",
            table_version,
            True,
            {
                "hoodie.copyonwrite.insert.auto.split": "false",
                "hoodie.copyonwrite.insert.split.size": "5",
            },
        )
        assert os.path.isdir(os.path.join(path, "x", "a"))
        assert os.path.isdir(os.path.join(path, "y", "a"))
        assert len(partition_file_ids(path, "x/a")) >= 2
        assert len(partition_file_ids(path, "y/a")) >= 2
        files = parquet_files(path)
        write_hudi_table(
            spark,
            path,
            [(i, "x/a", i * 100, 2) for i in range(5)],
            "insert_overwrite",
            table_version,
            True,
        )
        assert len(completed_replace_commits(path, table_version)) == 1
        assert files <= parquet_files(path)
        upload_hudi_table(minio_client, bucket, path, prefix)
        expected = [(i, "x/a", i * 100) for i in range(5)] + [
            (i, "y/a", i * 7) for i in range(15, 30)
        ]
        assert_rows(instance, source, "id, p, v", expected)

        # F: an instant that completes after the timeline was listed is not applied by that query.
        instance.query(f"CREATE TABLE {table} ENGINE = Hudi(s3, filename='{prefix}')")
        timeline = ".hoodie" if table_version == 6 else ".hoodie/timeline"
        instant = f"{prefix}{timeline}/99991231235959998.replacecommit"
        late = '{"partitionToReplaceFileIds": {"y/a": [%s]}}' % ", ".join(
            f'"{file_id}"' for file_id in sorted(partition_file_ids(path, "y/a"))
        )
        failpoint = "hudi_pause_after_listing_timeline"
        result = []
        errors = []

        def select():
            try:
                result.append(
                    instance.query(f"SELECT id, p, v FROM {table} ORDER BY id")
                )
            except BaseException as e:  # noqa: BLE001
                errors.append(e)

        instance.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        thread = threading.Thread(target=select)
        thread.start()
        try:
            instance.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
            put_object(minio_client, bucket, instant, late.encode())
            instance.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
        finally:
            instance.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            thread.join()
        if errors:
            raise errors[0]
        assert result == ["".join("\t".join(map(str, row)) + "\n" for row in expected)]
        assert_rows(
            instance, table, "id, p, v", [(i, "x/a", i * 100) for i in range(5)]
        )
        minio_client.remove_object(bucket, instant)
        assert_rows(instance, table, "id, p, v", expected)

        # insert_overwrite_table replaces the file groups of every partition.
        files = parquet_files(path)
        write_hudi_table(
            spark,
            path,
            [(i, "x/a", i * 1000, 3) for i in range(5)],
            "insert_overwrite_table",
            table_version,
            True,
        )
        assert len(completed_replace_commits(path, table_version)) == 2
        assert files <= parquet_files(path)
        upload_hudi_table(minio_client, bucket, path, prefix)
        expected = [(i, "x/a", i * 1000) for i in range(5)]
        assert_rows(instance, source, "id, p, v", expected)

        if table_version == 9:
            # C: instants that must not change the result of a layout 2 table: an empty
            # one, and one in the directory of the layout 1 timeline.
            put_object(
                minio_client,
                bucket,
                prefix
                + ".hoodie/timeline/99991231235959999_99991231235959999.replacecommit",
                b"",
            )
            assert_rows(instance, source, "id, p, v", expected)

            data_files = sorted(
                file
                for file in os.listdir(os.path.join(path, "x", "a"))
                if file.endswith(".parquet")
            )
            file_ids = sorted({file.split("_")[0] for file in data_files})
            replaced = '{"partitionToReplaceFileIds": {"x/a": [%s]}}' % ", ".join(
                f'"{file_id}"' for file_id in file_ids
            )
            put_object(
                minio_client,
                bucket,
                prefix + ".hoodie/99991231235959999.replacecommit",
                replaced.encode(),
            )
            assert_rows(instance, source, "id, p, v", expected)

            # The archived timeline of a layout 2 table is stored as Parquet.
            with open(os.path.join(path, "x", "a", data_files[0]), "rb") as f:
                put_object(
                    minio_client,
                    bucket,
                    prefix
                    + ".hoodie/timeline/history/00000000000000001_00000000000000002_0.parquet",
                    f.read(),
                )
            assert_rows(instance, source, "id, p, v", expected)

            # Java properties keep trailing whitespace and decode escapes.
            for new_line, accepted in [
                ("hoodie.timeline.path=custom_timeline", False),
                ("hoodie.timeline.path=timeline ", False),
                ("hoodie.timeline.path = timeline", True),
                ("hoodie.timeline.path=time\\u006Cine", True),
            ]:
                put_hudi_properties(
                    minio_client,
                    bucket,
                    path,
                    prefix,
                    "hoodie.timeline.path=timeline",
                    new_line,
                )
                if accepted:
                    assert_rows(instance, source, "id, p, v", expected)
                else:
                    error = instance.query_and_get_error(
                        f"SELECT count() FROM {source}"
                    )
                    assert "NOT_IMPLEMENTED" in error

            put_hudi_properties(
                minio_client,
                bucket,
                path,
                prefix,
                "hoodie.timeline.layout.version=2",
                "hoodie.timeline.layout.version=3",
            )
            error = instance.query_and_get_error(f"SELECT count() FROM {source}")
            assert "NOT_IMPLEMENTED" in error

            minio_client.remove_object(bucket, prefix + ".hoodie/hoodie.properties")
            minio_client.fput_object(
                bucket,
                prefix + ".hoodie/hoodie.properties.backup",
                os.path.join(path, ".hoodie", "hoodie.properties"),
            )
            assert_rows(instance, source, "id, p, v", expected)
    finally:
        instance.query(f"DROP TABLE IF EXISTS {table} SYNC")
        shutil.rmtree(tmp_dir, ignore_errors=True)
        remove_directory(minio_client, bucket, name)
        remove_directory(minio_client, bucket, f"{name}_partitioned")
        if minio_client.bucket_exists(root_bucket):
            for obj in minio_client.list_objects(root_bucket, recursive=True):
                minio_client.remove_object(root_bucket, obj.object_name)
            minio_client.remove_bucket(root_bucket)
