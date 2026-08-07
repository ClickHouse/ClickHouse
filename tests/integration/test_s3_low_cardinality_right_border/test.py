#!/usr/bin/env python3

#  1) Here we try to reproduce very rare error which is connected with
#  LowCardinality. When we read data from S3 we are trying to make sequential
#  reads without additional seeks. To achieve this we are trying to have as
#  large mark ranges as possible for each thread which read data from S3.
#  Additionaly, to avoid redundant reads we specify the "right border" for each
#  read. Such possiblity supported by S3 API. For example you can send request
#  to S3 to read data from 563 byte to 92753 byte and we use this feature in
#  ClickHouse.
#
#  2) We use granules (range of data between marks) as a minimal task for each
#  thread. For example, when we need to read data from 0 to 1092 mark and we
#  have two threads with one task for each of them: thread_1 = [0, 546),
#  thread_2 = [546, 1092). Of course S3 API knows nothing about marks, it works
#  with bytes. So, each marks points to some offset in compressed file (stored
#  in S3) and offset in decompressed block (here we don't need it). So to convert
#  our mark range into bytes range we use range.begin_mark.offset_in_compressed_file as
#  begin of bytes range and range.end.offset_in_compressed_file as end of bytes range. It
#  works most of the times, because this last mark in range is not included and we can its
#  offset_in_compressed_file as end for our range.
#
#  LowCardinality serialization format consist of two files (except files for marks):
#  file with index (column_name.bin) and file with dictionary (column_name.dict.bin). Data
#  in index file points to real column values in dictionary. Also dictionary can be shared between
#  several index marks (when you have a lot of rows with same value), for example:
#  ....
#  Mark 186, points to [2003111, 0]
#  Mark 187, points to [2003111, 0]
#  Mark 188, points to [2003111, 0]
#  Mark 189, points to [2003111, 0]
#  Mark 190, points to [2003111, 0]
#  Mark 191, points to [2003111, 0]
#  Mark 192, points to [2081424, 0]
#  Mark 193, points to [2081424, 0]
#  Mark 194, points to [2081424, 0]
#  Mark 195, points to [2081424, 0]
#  Mark 196, points to [2081424, 0]
#  Mark 197, points to [2081424, 0]
#  Mark 198, points to [2081424, 0]
#  Mark 199, points to [2081424, 0]
#  Mark 200, points to [2081424, 0]
#  Mark 201, points to [2159750, 0]
#  Mark 202, points to [2159750, 0]
#  Mark 203, points to [2159750, 0]
#  Mark 204, points to [2159750, 0]
#  ....
#
#  Imagine, this case when we have two threads: [0, 189) and [189, 378). Which
#  bytes range we will have? Using logic from 2) we will get
#  [0.offset_in_compressed_file, 189.offset_in_compressed_file] = [0, 2003111].
#  But it's incorrect range, because actually dictionary ends in offset 2081424,
#  but all marks from 186 to 191 share this same dictionary. If we try to read
#  data from [0, 2003111] we will not be able to do it, because it will be
#  impossible to read dictionary.
#
#  So this buggy logic was fixed and this test confirms this. At first I've
#  tried to get sane numbers for data, but the error didn't reproduce. After
#  three tries with almost random numbers of rows the error was reproduced.


import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", main_configs=["configs/s3.xml"], with_minio=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_s3_right_border(started_cluster):
    node1.query("drop table if exists s3_low_cardinality")
    node1.query(
        """
CREATE TABLE s3_low_cardinality
(
    str_column LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS storage_policy = 's3',  min_bytes_for_wide_part = 0, index_granularity = 1024;
    """
    )

    node1.query("INSERT INTO s3_low_cardinality SELECT 'aaaaaa' FROM numbers(600000)")
    node1.query(
        "INSERT INTO s3_low_cardinality SELECT toString(number) FROM numbers(100000)"
    )
    node1.query("INSERT INTO s3_low_cardinality SELECT 'bbbbbb' FROM numbers(500000)")
    node1.query(
        "INSERT INTO s3_low_cardinality SELECT toString(number + 100000000) FROM numbers(100000)"
    )

    node1.query("OPTIMIZE TABLE s3_low_cardinality FINAL")

    settings = {
        "merge_tree_min_bytes_for_concurrent_read": "0",
        "merge_tree_min_rows_for_concurrent_read": "0",
        "max_threads": "2",
    }
    assert (
        node1.query(
            "SELECT COUNT() FROM s3_low_cardinality WHERE not ignore(str_column)",
            settings=settings,
        )
        == "1300000\n"
    )


def test_s3_right_border_2(started_cluster):
    node1.query("drop table if exists s3_low_cardinality")
    node1.query(
        "create table s3_low_cardinality (key UInt32, str_column LowCardinality(String)) engine = MergeTree order by (key) settings storage_policy = 's3', min_bytes_for_wide_part = 0, index_granularity = 8192, min_compress_block_size=1, merge_max_block_size=10000"
    )
    node1.query(
        "insert into s3_low_cardinality select number, number % 8000 from numbers(8192)"
    )
    node1.query(
        "insert into s3_low_cardinality select number = 0 ? 0 : (number + 8192 * 1), number % 8000 + 1 * 8192 from numbers(8192)"
    )
    node1.query(
        "insert into s3_low_cardinality select number = 0 ? 0 : (number + 8192 * 2), number % 8000 + 2 * 8192 from numbers(8192)"
    )
    node1.query("optimize table s3_low_cardinality final")
    res = node1.query("select * from s3_low_cardinality where key = 9000")
    assert res == "9000\t9000\n"


def test_s3_right_border_3(started_cluster):
    node1.query("drop table if exists s3_low_cardinality")
    node1.query(
        "create table s3_low_cardinality (x LowCardinality(String)) engine = MergeTree order by tuple() settings min_bytes_for_wide_part=0, storage_policy = 's3', max_compress_block_size=10000"
    )
    node1.query(
        "insert into s3_low_cardinality select toString(number % 8000) || if(number < 8192 * 3, 'aaaaaaaaaaaaaaaa', if(number < 8192 * 6, 'bbbbbbbbbbbbbbbbbbbbbbbb', 'ccccccccccccccccccc')) from numbers(8192 * 9)"
    )
    # Marks are:
    # Mark 0, points to 0, 8, has rows after 8192, decompressed size 0.
    # Mark 1, points to 0, 8, has rows after 8192, decompressed size 0.
    # Mark 2, points to 0, 8, has rows after 8192, decompressed size 0.
    # Mark 3, points to 0, 8, has rows after 8192, decompressed size 0.
    # Mark 4, points to 42336, 2255, has rows after 8192, decompressed size 0.
    # Mark 5, points to 42336, 2255, has rows after 8192, decompressed size 0.
    # Mark 6, points to 42336, 2255, has rows after 8192, decompressed size 0.
    # Mark 7, points to 84995, 7738, has rows after 8192, decompressed size 0.
    # Mark 8, points to 84995, 7738, has rows after 8192, decompressed size 0.
    # Mark 9, points to 126531, 8637, has rows after 0, decompressed size 0.

    res = node1.query(
        "select uniq(x) from s3_low_cardinality settings max_threads=2, merge_tree_min_rows_for_concurrent_read_for_remote_filesystem=1, merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem=1"
    )
    # Reading ranges [0, 5) and [5, 9)

    assert res == "23999\n"
