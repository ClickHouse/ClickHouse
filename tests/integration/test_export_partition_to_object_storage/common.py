from helpers.export_partition_helpers import (
    is_replicated_engine,
    make_source,
)

# Table factories shared by the `EXPORT PARTITION` object-storage test modules.


def create_s3_table(node, s3_table):
    node.query(f"CREATE TABLE {s3_table} (id UInt64, year UInt16) ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive') PARTITION BY year")


def source_engine_clause(engine, table, replica_name="replica1"):
    """The `ENGINE` clause of a source table, for tests that spell out their own `CREATE`."""
    if is_replicated_engine(engine):
        return f"ReplicatedMergeTree('/clickhouse/tables/{table}', '{replica_name}')"
    return "MergeTree()"


def create_tables_and_insert_data(node, mt_table, s3_table, replica_name, engine="ReplicatedMergeTree"):
    node.query(f"DROP TABLE IF EXISTS {mt_table} SYNC")
    # enable_block_number_column and enable_block_offset_column are needed for patch parts support
    make_source(node, mt_table, "id UInt64, year UInt16", "year", engine=engine, replica_name=replica_name)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)")

    create_s3_table(node, s3_table)
