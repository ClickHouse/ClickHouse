import logging
import os

import pyspark
import pytest


from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import (
    AzureUploader,
    LocalUploader,
    S3Uploader,
    LocalDownloader,
    prepare_s3_bucket,
)
from helpers.test_tools import TSV

from helpers.iceberg_utils import (
    convert_schema_and_data_to_pandas_df,
    default_upload_directory,
    execute_spark_query_general,
    get_creation_expression,
    check_schema_and_data,
    get_raw_schema_and_data,
    get_uuid_str,
    get_spark,
    create_iceberg_table,
    default_download_directory,
)

from pandas.testing import assert_frame_equal

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/cluster.xml", "configs/config.d/named_collections.xml"],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        cluster.spark_session = get_spark()
        cluster.default_s3_uploader = S3Uploader(
            cluster.minio_client, cluster.minio_bucket
        )

        cluster.azure_container_name = "mycontainer"

        cluster.blob_service_client = cluster.blob_service_client

        container_client = cluster.blob_service_client.create_container(
            cluster.azure_container_name
        )

        cluster.container_client = container_client

        cluster.default_azure_uploader = AzureUploader(
            cluster.blob_service_client, cluster.azure_container_name
        )

        cluster.default_local_uploader = LocalUploader(cluster.instances["node1"])
        cluster.default_local_downloader = LocalDownloader(cluster.instances["node1"])

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
@pytest.mark.parametrize("is_table_function", [False, True])
def test_evolved_schema_simple(
    started_cluster, format_version, storage_type, is_table_function
):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_evolved_schema_simple_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster,
            storage_type,
            TABLE_NAME,
            query,
        )

    execute_spark_query(
        f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
        """
    )

    execute_spark_query(
        f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                a int NOT NULL,
                b float,
                c decimal(9,2) NOT NULL,
                d array<int>
            )
            USING iceberg
            OPTIONS ('format-version'='{format_version}')
        """
    )

    table_creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster,
        table_function=is_table_function,
    )

    table_select_expression = (
        TABLE_NAME if not is_table_function else table_creation_expression
    )

    if not is_table_function:
        instance.query(table_creation_expression)

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (4, NULL, 7.12, ARRAY(5, 6, 7));
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "\\N", "7.12", "[5,6,7]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b TYPE double;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float64)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "\\N", "7.12", "[5,6,7]"]],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (7, 5.0, 18.1, ARRAY(6, 7, 9));
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float64)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "\\N", "7.12", "[5,6,7]"], ["7", "5", "18.1", "[6,7,9]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN d FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["a", "Int32"],
            ["b", "Nullable(Float64)"],
            ["c", "Decimal(9, 2)"],
        ],
        [["[5,6,7]", "4", "\\N", "7.12"], ["[6,7,9]", "7", "5", "18.1"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b AFTER d;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(9, 2)"],
        ],
        [["[5,6,7]", "\\N", "4", "7.12"], ["[6,7,9]", "5", "7", "18.1"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME}
            ADD COLUMNS (
                e string
            );
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(9, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN c TYPE decimal(12, 2);
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(5, 6, 7), 3, -30, 7.12, 'AAA');
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a TYPE BIGINT;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int64"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(), 3.0, 12, -9.13, 'BBB');
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int64"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[]", "3", "12", "-9.13", "BBB"],
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a DROP NOT NULL;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[]", "3", "12", "-9.13", "BBB"],
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (NULL, 3.4, NULL, -9.13, NULL);
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[]", "3", "12", "-9.13", "BBB"],
            ["[]", "3.4", "\\N", "-9.13", "\\N"],
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN d;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["b", "Nullable(Float64)"],
            ["a", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["3", "-30", "7.12", "AAA"],
            ["3", "12", "-9.13", "BBB"],
            ["3.4", "\\N", "-9.13", "\\N"],
            ["5", "7", "18.1", "\\N"],
            ["\\N", "4", "7.12", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN a TO f;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["b", "Nullable(Float64)"],
            ["f", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["3", "-30", "7.12", "AAA"],
            ["3", "12", "-9.13", "BBB"],
            ["3.4", "\\N", "-9.13", "\\N"],
            ["5", "7", "18.1", "\\N"],
            ["\\N", "4", "7.12", "\\N"],
        ],
    )
    if not is_table_function :
        print (instance.query("SELECT * FROM system.iceberg_history"))
        assert int(instance.query(f"SELECT count() FROM system.iceberg_history WHERE table = '{TABLE_NAME}'")) == 5
        assert int(instance.query(f"SELECT count() FROM system.iceberg_history WHERE table = '{TABLE_NAME}' AND made_current_at >= yesterday()")) == 5

    # Do a single check to verify that restarting CH maintains the setting (ATTACH)
    # We are just interested on the setting working after restart, so no need to run it on all combinations
    if format_version == "1" and storage_type == "s3" and not is_table_function:

        instance.restart_clickhouse()

        execute_spark_query(
            f"""
                ALTER TABLE {TABLE_NAME} RENAME COLUMN e TO z;
            """
        )

        check_schema_and_data(
            instance,
            table_select_expression,
            [
                ["b", "Nullable(Float64)"],
                ["f", "Nullable(Int64)"],
                ["c", "Decimal(12, 2)"],
                ["z", "Nullable(String)"],
            ],
            [
                ["3", "-30", "7.12", "AAA"],
                ["3", "12", "-9.13", "BBB"],
                ["3.4", "\\N", "-9.13", "\\N"],
                ["5", "7", "18.1", "\\N"],
                ["\\N", "4", "7.12", "\\N"],
            ],
        )


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_array_evolved_with_struct(
    started_cluster, format_version, storage_type
):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_array_evolved_with_struct_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )
        return

    execute_spark_query(
        f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
        """
    )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME}   (
                address ARRAY<STRUCT<
                    city: STRING,
                    zip: INT
                >>,
                values ARRAY<INT>
            )
            USING iceberg
            OPTIONS ('format-version'='{format_version}')
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(named_struct('name', 'Singapore', 'zip', 12345), named_struct('name', 'Moscow', 'zip', 54321)), ARRAY(1,2));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster, table_function=True
    )
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMNS ( address.element.foo INT );
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    city Nullable(String),\\n    zip Nullable(Int32),\\n    foo Nullable(Int32)))'],
            ['values', 'Array(Nullable(Int32))']
        ],
        [
            ["[('Singapore',12345,NULL),('Moscow',54321,NULL)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN address.element.city;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    zip Nullable(Int32),\\n    foo Nullable(Int32)))'],
            ['values', 'Array(Nullable(Int32))']
        ],
        [
            ["[(12345,NULL),(54321,NULL)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN address.element.foo FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    foo Nullable(Int32),\\n    zip Nullable(Int32)))'],
            ['values', 'Array(Nullable(Int32))']
        ],
        [
            ["[(NULL,12345),(NULL,54321)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN address.element.foo TO city;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    city Nullable(Int32),\\n    zip Nullable(Int32)))'],
            ['values', 'Array(Nullable(Int32))']
        ],
        [
            ["[(NULL,12345),(NULL,54321)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN address TO bee;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['bee', 'Array(Tuple(\\n    city Nullable(Int32),\\n    zip Nullable(Int32)))'],
            ['values', 'Array(Nullable(Int32))']
        ],
        [
            ["[(NULL,12345),(NULL,54321)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN values TO fee;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['bee', 'Array(Tuple(\\n    city Nullable(Int32),\\n    zip Nullable(Int32)))'],
            ['fee', 'Array(Nullable(Int32))']
        ],
        [
            ["[(NULL,12345),(NULL,54321)]", '[1,2]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN fee.element TYPE long;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['bee', 'Array(Tuple(\\n    city Nullable(Int32),\\n    zip Nullable(Int32)))'],
            ['fee', 'Array(Nullable(Int64))']
        ],
        [
            ["[(NULL,12345),(NULL,54321)]", '[1,2]']
        ],
    )
    return
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN fee FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['fee', 'Array(Nullable(Int64))'],
            ['bee', 'Array(Tuple(\\n    city Nullable(Int32),\\n    zip Nullable(Int32)))']
        ],
        [
            ['[1,2]', "[(NULL,12345),(NULL,54321)]"]
        ],
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_array_evolved_nested(
    started_cluster, format_version, storage_type
):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_array_evolved_nested_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )
        return

    execute_spark_query(
        f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
        """
    )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME}   (
                address ARRAY<STRUCT<
                    city: STRUCT<
                        foo: STRING,
                        bar: INT
                    >,
                    zip: ARRAY<INT>
                >>
            )
            USING iceberg
            OPTIONS ('format-version'='{format_version}')
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(named_struct('city', named_struct('foo', 'some_value', 'bar', 40), 'zip', ARRAY(41,42)), named_struct('city', named_struct('foo', 'some_value2', 'bar', 1), 'zip', ARRAY(2,3,4))));
        """
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMNS ( address.element.zap INT );
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster, table_function=True
    )
    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    city Tuple(\\n        foo Nullable(String),\\n        bar Nullable(Int32)),\\n    zip Array(Nullable(Int32)),\\n    zap Nullable(Int32)))']
        ],
        [
            ["[(('some_value',40),[41,42],NULL),(('some_value2',1),[2,3,4],NULL)]"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN address.element.zip.element TYPE long;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    city Tuple(\\n        foo Nullable(String),\\n        bar Nullable(Int32)),\\n    zip Array(Nullable(Int64)),\\n    zap Nullable(Int32)))']
        ],
        [
            ["[(('some_value',40),[41,42],NULL),(('some_value2',1),[2,3,4],NULL)]"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN address.element.zip FIRST;
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(named_struct('zip', ARRAY(411,421), 'city', named_struct('foo', 'some_value1', 'bar', 401), 'zap', 3), named_struct('zip', ARRAY(21,31,41), 'city', named_struct('foo', 'some_value21', 'bar', 11), 'zap', 4)));
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    zip Array(Nullable(Int64)),\\n    city Tuple(\\n        foo Nullable(String),\\n        bar Nullable(Int32)),\\n    zap Nullable(Int32)))']
        ],
        [
            ["[([41,42],('some_value',40),NULL),([2,3,4],('some_value2',1),NULL)]"],
            ["[([411,421],('some_value1',401),3),([21,31,41],('some_value21',11),4)]"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN address.element.city.foo;
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(named_struct('zip', ARRAY(4111,4211), 'city', named_struct('bar', 4011), 'zap', 31), named_struct('zip', ARRAY(211,311,411), 'city', named_struct('bar', 111), 'zap', 41)));
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    zip Array(Nullable(Int64)),\\n    city Tuple(\\n        bar Nullable(Int32)),\\n    zap Nullable(Int32)))']
        ],
        [
            ["[([41,42],(40),NULL),([2,3,4],(1),NULL)]"],
            ["[([411,421],(401),3),([21,31,41],(11),4)]"],
            ["[([4111,4211],(4011),31),([211,311,411],(111),41)]"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN address.element.zap FIRST;
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(named_struct('zap', 32, 'zip', ARRAY(4112,4212), 'city', named_struct('bar', 4012)), named_struct('zap', 42, 'zip', ARRAY(212,312,412), 'city', named_struct('bar', 112))));
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    zap Nullable(Int32),\\n    zip Array(Nullable(Int64)),\\n    city Tuple(\\n        bar Nullable(Int32))))']
        ],
        [
            ["[(3,[411,421],(401)),(4,[21,31,41],(11))]"],
            ["[(31,[4111,4211],(4011)),(41,[211,311,411],(111))]"],
            ["[(32,[4112,4212],(4012)),(42,[212,312,412],(112))]"],
            ["[(NULL,[41,42],(40)),(NULL,[2,3,4],(1))]"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMNS ( address.element.city.newbar INT );
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(named_struct('zap', 33, 'zip', ARRAY(4113,4213), 'city', named_struct('bar', 4013, 'newbar', 5013)), named_struct('zap', 43, 'zip', ARRAY(213,313,413), 'city', named_struct('bar', 113, 'newbar', 513))));
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    zap Nullable(Int32),\\n    zip Array(Nullable(Int64)),\\n    city Tuple(\\n        bar Nullable(Int32),\\n        newbar Nullable(Int32))))']
        ],
        [
            ["[(3,[411,421],(401,NULL)),(4,[21,31,41],(11,NULL))]"],
            ["[(31,[4111,4211],(4011,NULL)),(41,[211,311,411],(111,NULL))]"],
            ["[(32,[4112,4212],(4012,NULL)),(42,[212,312,412],(112,NULL))]"],
            ["[(33,[4113,4213],(4013,5013)),(43,[213,313,413],(113,513))]"],
            ["[(NULL,[41,42],(40,NULL)),(NULL,[2,3,4],(1,NULL))]"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMNS ( address.element.new_tuple struct<new_tuple_elem:INT, new_tuple_elem2:INT> );
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(named_struct('zap', 34, 'zip', ARRAY(4114,4214), 'city', named_struct('bar', 4014, 'newbar', 5014), 'new_tuple', named_struct('new_tuple_elem',4,'new_tuple_elem2',4)), named_struct('zap', 44, 'zip', ARRAY(214,314,414), 'city', named_struct('bar', 114, 'newbar', 514), 'new_tuple', named_struct('new_tuple_elem',4,'new_tuple_elem2',4))));
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    zap Nullable(Int32),\\n    zip Array(Nullable(Int64)),\\n    city Tuple(\\n        bar Nullable(Int32),\\n        newbar Nullable(Int32)),\\n    new_tuple Tuple(\\n        new_tuple_elem Nullable(Int32),\\n        new_tuple_elem2 Nullable(Int32))))']
        ],
        [
            ["[(3,[411,421],(401,NULL),(NULL,NULL)),(4,[21,31,41],(11,NULL),(NULL,NULL))]"],
            ["[(31,[4111,4211],(4011,NULL),(NULL,NULL)),(41,[211,311,411],(111,NULL),(NULL,NULL))]"],
            ["[(32,[4112,4212],(4012,NULL),(NULL,NULL)),(42,[212,312,412],(112,NULL),(NULL,NULL))]"],
            ["[(33,[4113,4213],(4013,5013),(NULL,NULL)),(43,[213,313,413],(113,513),(NULL,NULL))]"],
            ["[(34,[4114,4214],(4014,5014),(4,4)),(44,[214,314,414],(114,514),(4,4))]"],
            ["[(NULL,[41,42],(40,NULL),(NULL,NULL)),(NULL,[2,3,4],(1,NULL),(NULL,NULL))]"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN address.element.city.newbar FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    zap Nullable(Int32),\\n    zip Array(Nullable(Int64)),\\n    city Tuple(\\n        newbar Nullable(Int32),\\n        bar Nullable(Int32)),\\n    new_tuple Tuple(\\n        new_tuple_elem Nullable(Int32),\\n        new_tuple_elem2 Nullable(Int32))))']
        ],
        [
            ["[(3,[411,421],(NULL,401),(NULL,NULL)),(4,[21,31,41],(NULL,11),(NULL,NULL))]"],
            ["[(31,[4111,4211],(NULL,4011),(NULL,NULL)),(41,[211,311,411],(NULL,111),(NULL,NULL))]"],
            ["[(32,[4112,4212],(NULL,4012),(NULL,NULL)),(42,[212,312,412],(NULL,112),(NULL,NULL))]"],
            ["[(33,[4113,4213],(5013,4013),(NULL,NULL)),(43,[213,313,413],(513,113),(NULL,NULL))]"],
            ["[(34,[4114,4214],(5014,4014),(4,4)),(44,[214,314,414],(514,114),(4,4))]"],
            ["[(NULL,[41,42],(NULL,40),(NULL,NULL)),(NULL,[2,3,4],(NULL,1),(NULL,NULL))]"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN address.element.city FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    city Tuple(\\n        newbar Nullable(Int32),\\n        bar Nullable(Int32)),\\n    zap Nullable(Int32),\\n    zip Array(Nullable(Int64)),\\n    new_tuple Tuple(\\n        new_tuple_elem Nullable(Int32),\\n        new_tuple_elem2 Nullable(Int32))))']
        ],
        [
            ["[((5013,4013),33,[4113,4213],(NULL,NULL)),((513,113),43,[213,313,413],(NULL,NULL))]"],
            ["[((5014,4014),34,[4114,4214],(4,4)),((514,114),44,[214,314,414],(4,4))]"],
            ["[((NULL,40),NULL,[41,42],(NULL,NULL)),((NULL,1),NULL,[2,3,4],(NULL,NULL))]"],
            ["[((NULL,401),3,[411,421],(NULL,NULL)),((NULL,11),4,[21,31,41],(NULL,NULL))]"],
            ["[((NULL,4011),31,[4111,4211],(NULL,NULL)),((NULL,111),41,[211,311,411],(NULL,NULL))]"],
            ["[((NULL,4012),32,[4112,4212],(NULL,NULL)),((NULL,112),42,[212,312,412],(NULL,NULL))]"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN address.element.city.bar;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    city Tuple(\\n        newbar Nullable(Int32)),\\n    zap Nullable(Int32),\\n    zip Array(Nullable(Int64)),\\n    new_tuple Tuple(\\n        new_tuple_elem Nullable(Int32),\\n        new_tuple_elem2 Nullable(Int32))))']
        ],
        [
            ["[((5013),33,[4113,4213],(NULL,NULL)),((513),43,[213,313,413],(NULL,NULL))]"],
            ["[((5014),34,[4114,4214],(4,4)),((514),44,[214,314,414],(4,4))]"],
            ["[((NULL),3,[411,421],(NULL,NULL)),((NULL),4,[21,31,41],(NULL,NULL))]"],
            ["[((NULL),31,[4111,4211],(NULL,NULL)),((NULL),41,[211,311,411],(NULL,NULL))]"],
            ["[((NULL),32,[4112,4212],(NULL,NULL)),((NULL),42,[212,312,412],(NULL,NULL))]"],
            ["[((NULL),NULL,[41,42],(NULL,NULL)),((NULL),NULL,[2,3,4],(NULL,NULL))]"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN address.element.zip;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    city Tuple(\\n        newbar Nullable(Int32)),\\n    zap Nullable(Int32),\\n    new_tuple Tuple(\\n        new_tuple_elem Nullable(Int32),\\n        new_tuple_elem2 Nullable(Int32))))']
        ],
        [
            ["[((5013),33,(NULL,NULL)),((513),43,(NULL,NULL))]"],
            ["[((5014),34,(4,4)),((514),44,(4,4))]"],
            ["[((NULL),3,(NULL,NULL)),((NULL),4,(NULL,NULL))]"],
            ["[((NULL),31,(NULL,NULL)),((NULL),41,(NULL,NULL))]"],
            ["[((NULL),32,(NULL,NULL)),((NULL),42,(NULL,NULL))]"],
            ["[((NULL),NULL,(NULL,NULL)),((NULL),NULL,(NULL,NULL))]"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN address.element.city;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Array(Tuple(\\n    zap Nullable(Int32),\\n    new_tuple Tuple(\\n        new_tuple_elem Nullable(Int32),\\n        new_tuple_elem2 Nullable(Int32))))']
        ],
        [
            ["[(3,(NULL,NULL)),(4,(NULL,NULL))]"],
            ["[(31,(NULL,NULL)),(41,(NULL,NULL))]"],
            ["[(32,(NULL,NULL)),(42,(NULL,NULL))]"],
            ["[(33,(NULL,NULL)),(43,(NULL,NULL))]"],
            ["[(34,(4,4)),(44,(4,4))]"],
            ["[(NULL,(NULL,NULL)),(NULL,(NULL,NULL))]"],
        ],
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
@pytest.mark.parametrize("is_table_function", [False, True])
def test_tuple_evolved_nested(
    started_cluster, format_version, storage_type, is_table_function
):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_tuple_evolved_nested_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )
        return

    execute_spark_query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    execute_spark_query(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            a int NOT NULL,
            b struct<a: float, b: struct<na: float, nb: string>>,
            c struct<c : int, d: int>
        )
        USING iceberg 
        OPTIONS ('format-version'='2')
    """)

    execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES (1, named_struct('a', 1.23, 'b', named_struct('na', 4.56, 'nb', 'BACCARA')), named_struct('c', 1, 'd', 2))")

    table_creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster,
        table_function=is_table_function,
    )

    table_select_expression = (
        TABLE_NAME if not is_table_function else table_creation_expression
    )

    if not is_table_function:
        instance.query(table_creation_expression)


    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        na Nullable(Float32),\\n        nb Nullable(String)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(4.56,'BACCARA'))", '(1,2)']
        ],
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} RENAME COLUMN b.b.na TO e")

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        e Nullable(Float32),\\n        nb Nullable(String)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(4.56,'BACCARA'))", '(1,2)']
        ],
    )

    execute_spark_query(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN b.b.e TYPE double;")

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        e Nullable(Float64),\\n        nb Nullable(String)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(4.559999942779541,'BACCARA'))", '(1,2)']
        ],
    )
    execute_spark_query(f"ALTER TABLE {TABLE_NAME} DROP COLUMN b.b.nb")

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        e Nullable(Float64)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(4.559999942779541))", '(1,2)']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMN b.b.nc int;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        e Nullable(Float64),\\n        nc Nullable(Int32)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(4.559999942779541,NULL))", '(1,2)']
        ],
    )
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b.b.nc FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['a', 'Int32'], 
            ['b', 'Tuple(\\n    a Nullable(Float32),\\n    b Tuple(\\n        nc Nullable(Int32),\\n        e Nullable(Float64)))'],
            ['c', 'Tuple(\\n    c Nullable(Int32),\\n    d Nullable(Int32))']
        ],
        [
            ['1', "(1.23,(NULL,4.559999942779541))", '(1,2)']
        ],
    )

@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["local"])
@pytest.mark.parametrize("is_table_function", [False])
def test_map_evolved_nested(
    started_cluster, format_version, storage_type, is_table_function
):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_tuple_evolved_nested_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )
        return

    execute_spark_query(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    execute_spark_query(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            b Map<INT, INT>,
            a Map<INT, Struct<
                c : INT,
                d : String
            >>,
            c Struct <
                e : Map<Int, String>
            >
        )
        USING iceberg 
        OPTIONS ('format-version'='2')
    """)

    execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES (MAP(1, 2), Map(3, named_struct('c', 4, 'd', 'ABBA')), named_struct('e', MAP(5, 'foo')))")

    table_creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster,
        table_function=is_table_function,
    )

    table_select_expression = (
        TABLE_NAME if not is_table_function else table_creation_expression
    )

    if not is_table_function:
        instance.query(table_creation_expression)

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b.value TYPE long;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))'],
            ['c', 'Tuple(\\n    e Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:(4,'ABBA')}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN c.e TO f;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:(4,'ABBA')}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a.value.d FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    d Nullable(String),\\n    c Nullable(Int32)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:('ABBA',4)}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMN a.value.g int;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    d Nullable(String),\\n    c Nullable(Int32),\\n    g Nullable(Int32)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:('ABBA',4,NULL)}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a.value.g FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    g Nullable(Int32),\\n    d Nullable(String),\\n    c Nullable(Int32)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:(NULL,'ABBA',4)}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN a.value.c;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    g Nullable(Int32),\\n    d Nullable(String)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:(NULL,'ABBA')}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN a.value.g TO c;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))'],
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))']
        ],
        [
            ['{1:2}', "{3:(NULL,'ABBA')}", "({5:'foo'})"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN c FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)))'],
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))']
        ],
        [
            ["({5:'foo'})", '{1:2}', "{3:(NULL,'ABBA')}"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMN c.g int;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['c', 'Tuple(\\n    f Map(Int32, Nullable(String)),\\n    g Nullable(Int32))'],
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))']
        ],
        [
            ["({5:'foo'},NULL)", '{1:2}', "{3:(NULL,'ABBA')}"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN c.g FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['c', 'Tuple(\\n    g Nullable(Int32),\\n    f Map(Int32, Nullable(String)))'],
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))']
        ],
        [
            ["(NULL,{5:'foo'})", '{1:2}', "{3:(NULL,'ABBA')}"]
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN c.f;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ['c', 'Tuple(\\n    g Nullable(Int32))'],
            ['b', 'Map(Int32, Nullable(Int64))'], 
            ['a', 'Map(Int32, Tuple(\\n    c Nullable(Int32),\\n    d Nullable(String)))']
        ],
        [
            ["(NULL)", '{1:2}', "{3:(NULL,'ABBA')}"]
        ],
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_evolved_schema_complex(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_evolved_schema_complex_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster,
            storage_type,
            TABLE_NAME,
            query,
        )

    execute_spark_query(
        f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
        """
    )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME}   (
                address STRUCT<
                    house_number : DOUBLE,
                    city: STRUCT<
                        name: STRING,
                        zip: INT
                    >
                >,
                animals ARRAY<INT>
            )
            USING iceberg
            OPTIONS ('format-version'='{format_version}')
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (named_struct('house_number', 3, 'city', named_struct('name', 'Singapore', 'zip', 12345)), ARRAY(4, 7));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster, table_function=True
    )
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMNS ( address.appartment INT );
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 
             'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)),\\n    appartment Nullable(Int32))'],
            ['animals',
                'Array(Nullable(Int32))'],
        ],
        [
            ["(3,('Singapore',12345),NULL)", '[4,7]']
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN address.appartment;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 
             'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)))'],
            ["animals", "Array(Nullable(Int32))"],
        ],
        [["(3,('Singapore',12345))", "[4,7]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN animals.element TYPE BIGINT
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)))'],
            ['animals',
                'Array(Nullable(Int64))'],
        ],
        [
           ["(3,('Singapore',12345))", '[4,7]']
        ]
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMNS ( map_column Map<INT, INT> );
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (named_struct('house_number', 4, 'city', named_struct('name', 'Moscow', 'zip', 54321)), ARRAY(4, 7), MAP(1, 2));
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)))'],
            ['animals',
                'Array(Nullable(Int64))'],
            ['map_column', 'Map(Int32, Nullable(Int32))']
        ],
        [
           ["(3,('Singapore',12345))", '[4,7]', '{}'],
           ["(4,('Moscow',54321))", '[4,7]', '{1:2}'],
        ]
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN map_column TO col_to_del;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)))'],
            ['animals',
                'Array(Nullable(Int64))'],
            ['col_to_del', 'Map(Int32, Nullable(Int32))']
        ],
        [
           ["(3,('Singapore',12345))", '[4,7]', '{}'],
           ["(4,('Moscow',54321))", '[4,7]', '{1:2}'],
        ]
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN col_to_del;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            ['address', 'Tuple(\\n    house_number Nullable(Float64),\\n    city Tuple(\\n        name Nullable(String),\\n        zip Nullable(Int32)))'],
            ['animals',
                'Array(Nullable(Int64))']
        ],
        [
           ["(3,('Singapore',12345))", '[4,7]'],
           ["(4,('Moscow',54321))", '[4,7]'],
        ]
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_correct_column_mapper_is_chosen(
    started_cluster,
    storage_type,
    format_version
):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_correct_column_mapper_is_chosen_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (
            a int NOT NULL,
            b int NOT NULL
        )
        USING iceberg
        OPTIONS ('format-version'='{format_version}');
        """
    )

    spark.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES (0, 1);
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {TABLE_NAME} RENAME COLUMN b TO c;
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {TABLE_NAME} RENAME COLUMN a TO b;
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {TABLE_NAME} RENAME COLUMN c TO a;
        """
    )

    spark.sql(
        f"""
        ALTER TABLE {TABLE_NAME} ALTER COLUMN b AFTER a;
        """
    )
    
    spark.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES (1, 0);
        """
    )

    table_function = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster,
        table_function=True,
        is_cluster=True,
    )


    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    raw_schema, raw_data = get_raw_schema_and_data(instance, table_function)

    _schema_df, data_df = convert_schema_and_data_to_pandas_df(raw_schema, raw_data)

    reference_df = spark.sql(
        f"""
        SELECT * FROM {TABLE_NAME} ORDER BY a;
        """
    ).toPandas()

    assert_frame_equal(data_df, reference_df)

@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_full_drop(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_full_drop_" + storage_type + "_" + get_uuid_str()
    TABLE_NAME_2 = "test_full_drop_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster, "(x Nullable(Int32))", format_version)
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n'

    create_iceberg_table(storage_type, instance, TABLE_NAME_2, started_cluster, "(x Nullable(Int32))", format_version)
    assert instance.query(f"SELECT * FROM {TABLE_NAME_2} ORDER BY ALL") == ''
    instance.query(f"INSERT INTO {TABLE_NAME_2} VALUES (777);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME_2} ORDER BY ALL") == '777\n'

    instance.query(f"DROP TABLE {TABLE_NAME}")

    #exception means that there are no files.
    with pytest.raises(Exception):
        default_download_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

    assert instance.query(f"SELECT * FROM {TABLE_NAME_2} ORDER BY ALL") == '777\n'
