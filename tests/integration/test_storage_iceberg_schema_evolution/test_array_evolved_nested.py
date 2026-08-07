
import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
    check_schema_and_data,
    default_upload_directory,
    get_creation_expression
)

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_array_evolved_nested(
    started_cluster_iceberg_schema_evolution, format_version, storage_type
):
    instance = started_cluster_iceberg_schema_evolution.instances["node1"]
    spark = started_cluster_iceberg_schema_evolution.spark_session
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
            started_cluster_iceberg_schema_evolution,
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
        storage_type, TABLE_NAME, started_cluster_iceberg_schema_evolution, table_function=True
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
