<fragment>
    <create_query>CREATE DATABASE IF NOT EXISTS {database}</create_query>


    <!-- wr_large: insert_large target; the baseline fill makes every measured run append to a non-empty table. -->
    <create_query>DROP TABLE IF EXISTS {database}.wr_large</create_query>
    <create_query>
        CREATE TABLE {database}.wr_large
        (
            l_orderkey Int32, l_partkey Int32, l_suppkey Int32, l_linenumber Int32,
            l_quantity Float64, l_extendedprice Float64, l_discount Float64, l_tax Float64,
            l_returnflag String, l_linestatus String,
            l_shipdate Date, l_commitdate Date, l_receiptdate Date,
            l_shipinstruct String, l_shipmode String, l_comment String
        )
        ENGINE = {engine_function_wr_large}
    </create_query>
    <create_query>INSERT INTO {database}.wr_large SELECT * FROM tpch10.lineitem LIMIT 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>

    <!-- insert_large: 200K rows as one block/commit, so Parquet encoding dominates the per-run cost. -->
    <query>
        INSERT INTO {database}.wr_large SELECT * FROM tpch10.lineitem LIMIT 200000
        SETTINGS max_threads = 1, max_insert_threads = 1,
                 min_insert_block_size_rows = 200000, min_insert_block_size_bytes = 0
    </query>


    <!-- wr_small: write_small_commits.sql target; baseline fill for the same reason. -->
    <create_query>DROP TABLE IF EXISTS {database}.wr_small</create_query>
    <create_query>
        CREATE TABLE {database}.wr_small
        (
            l_orderkey Int32, l_partkey Int32, l_suppkey Int32, l_linenumber Int32,
            l_quantity Float64, l_extendedprice Float64, l_discount Float64, l_tax Float64,
            l_returnflag String, l_linestatus String,
            l_shipdate Date, l_commitdate Date, l_receiptdate Date,
            l_shipinstruct String, l_shipmode String, l_comment String
        )
        ENGINE = {engine_function_wr_small}
    </create_query>
    <create_query>INSERT INTO {database}.wr_small SELECT * FROM tpch10.lineitem LIMIT 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>

    <!-- insert_small_commits: 5 tiny commits per run to pressure metadata updates -->
    <query file="write_small_commits.sql"/>


    <!-- wr_planned: fixed ten-snapshot append-only history shared by the two planning-only queries below, which commit nothing and so never change it. -->
    <create_query>DROP TABLE IF EXISTS {database}.wr_planned</create_query>
    <create_query>
        CREATE TABLE {database}.wr_planned
        (
            l_orderkey Int32, l_partkey Int32, l_suppkey Int32, l_linenumber Int32,
            l_quantity Float64, l_extendedprice Float64, l_discount Float64, l_tax Float64,
            l_returnflag String, l_linestatus String,
            l_shipdate Date, l_commitdate Date, l_receiptdate Date,
            l_shipinstruct String, l_shipmode String, l_comment String
        )
        ENGINE = {engine_function_wr_planned}
    </create_query>
    <create_query>INSERT INTO {database}.wr_planned SELECT * FROM tpch10.lineitem LIMIT 0, 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>
    <create_query>INSERT INTO {database}.wr_planned SELECT * FROM tpch10.lineitem LIMIT 100000, 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>
    <create_query>INSERT INTO {database}.wr_planned SELECT * FROM tpch10.lineitem LIMIT 200000, 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>
    <create_query>INSERT INTO {database}.wr_planned SELECT * FROM tpch10.lineitem LIMIT 300000, 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>
    <create_query>INSERT INTO {database}.wr_planned SELECT * FROM tpch10.lineitem LIMIT 400000, 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>
    <create_query>INSERT INTO {database}.wr_planned SELECT * FROM tpch10.lineitem LIMIT 500000, 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>
    <create_query>INSERT INTO {database}.wr_planned SELECT * FROM tpch10.lineitem LIMIT 600000, 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>
    <create_query>INSERT INTO {database}.wr_planned SELECT * FROM tpch10.lineitem LIMIT 700000, 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>
    <create_query>INSERT INTO {database}.wr_planned SELECT * FROM tpch10.lineitem LIMIT 800000, 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>
    <create_query>INSERT INTO {database}.wr_planned SELECT * FROM tpch10.lineitem LIMIT 900000, 100000 SETTINGS max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0</create_query>

    <!-- optimize_table: with an append-only history and no position deletes nothing is rewritten, so every run measures the planning pass over the fixed ten snapshots. -->
    <query>OPTIMIZE TABLE {database}.wr_planned SETTINGS allow_experimental_iceberg_compaction = 1</query>

    <!-- update_no_match: mutation planning only - per-file stats refute the predicate, so nothing is scanned or rewritten and no snapshot is committed. -->
    <query><![CDATA[ALTER TABLE {database}.wr_planned UPDATE l_quantity = l_quantity + 1 WHERE l_partkey < 0]]></query>


    <drop_query>DROP TABLE IF EXISTS {database}.wr_large</drop_query>
    <drop_query>DROP TABLE IF EXISTS {database}.wr_small</drop_query>
    <drop_query>DROP TABLE IF EXISTS {database}.wr_planned</drop_query>
</fragment>
