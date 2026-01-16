import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=["configs/users.xml"],
)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_check_data_size(start_cluster):
    node.query("DROP TABLE IF EXISTS tab_bitpacking;") 
    node.query("DROP TABLE IF EXISTS tab_uncompressed;") 
    node.query(
            """
            CREATE TABLE tab_bitpacking
            (
             ts DateTime,
             str String,
             INDEX inv_idx str TYPE text(
                 tokenizer = 'splitByNonAlpha',
                 posting_list_codec = 'bitpacking'
                 )
            )
            ENGINE = MergeTree
            ORDER BY ts
            SETTINGS
                min_rows_for_wide_part = 0,
                min_bytes_for_wide_part= 0,
                index_granularity = 8192,
                index_granularity_bytes = 0,
                enable_block_offset_column = 0,
                enable_block_number_column = 0,
                string_serialization_version = 'with_size_stream',
                primary_key_compress_block_size = 65536,
                marks_compress_block_size = 65536,
                ratio_of_defaults_for_sparse_serialization = 0.95,
                serialization_info_version = 'basic',
                auto_statistics_types = 'minmax';
            """
    )
    node.query(
            """
            CREATE TABLE tab_uncompressed
            (
             ts DateTime,
             str String,
             INDEX inv_idx str TYPE text(
                 tokenizer = 'splitByNonAlpha',
                 posting_list_codec = 'bitpacking'
                 )
            )
            ENGINE = MergeTree
            ORDER BY ts
            SETTINGS
                min_rows_for_wide_part = 0,
                min_bytes_for_wide_part= 0,
                index_granularity = 8192,
                index_granularity_bytes = 0,
                enable_block_offset_column = 0,
                enable_block_number_column = 0,
                string_serialization_version = 'with_size_stream',
                primary_key_compress_block_size = 65536,
                marks_compress_block_size = 65536,
                ratio_of_defaults_for_sparse_serialization = 0.95,
                serialization_info_version = 'basic',
                auto_statistics_types = 'minmax';
            """
    )
    node.query("INSERT INTO tab_bitpacking SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') AS str FROM numbers(1024000);")
    node.query("INSERT INTO tab_uncompressed SELECT '2026-01-09 10:00:00', multiIf(number % 3 = 0, 'aa', number % 3 = 1, 'bb', 'cc') AS str FROM numbers(1024000);")
   
    node.query("""
        INSERT INTO tab_bitpacking
        SELECT
        '2026-01-09 11:00:00',
        multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
        FROM numbers(1024000);
        """)
    node.query("""
        INSERT INTO tab_uncompressed
        SELECT
        '2026-01-09 11:00:00',
        multiIf(number % 3 = 0, 'aa',
            number % 3 = 1, 'bb',
            'cc') AS str
        FROM numbers(1024000);
        """)
    node.query("""
        INSERT INTO tab_bitpacking
        SELECT
        '2026-01-09 12:00:00',
        multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
        FROM numbers(512);
        """)
    node.query("""
        INSERT INTO tab_uncompressed
        SELECT
        '2026-01-09 12:00:00',
        multiIf(number < 129, 'tail129',
            number = 129, 'single',
            'noise') AS str
        FROM numbers(512);
        """)
    node.query("""
        INSERT INTO tab_bitpacking
        SELECT
        '2026-01-09 14:00:00',
        if(number < 1003, 'mid1003', 'noise') AS str
        FROM numbers(1500);
        """)
    node.query("""
        INSERT INTO tab_uncompressed
        SELECT
        '2026-01-09 14:00:00',
        if(number < 1003, 'mid1003', 'noise') AS str
        FROM numbers(1500);
        """)
    node.query("""
        INSERT INTO tab_bitpacking
        SELECT
        '2026-01-09 16:00:00',
        multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS str
        FROM numbers(2000);
        """)
    node.query("""
        INSERT INTO tab_uncompressed
        SELECT
        '2026-01-09 16:00:00',
        multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS str
        FROM numbers(2000);
        """)
    node.query("""
        INSERT INTO tab_bitpacking
        SELECT
        '2026-01-09 16:00:00',
        multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS str
        FROM numbers(2000);
        """)
    node.query("""
        INSERT INTO tab_uncompressed
        SELECT
        '2026-01-09 16:00:00',
        multiIf(number IN (0, 777), 'rare2',
            number IN (1, 2, 3, 4, 5), 'rare5',
            'noise') AS str
        FROM numbers(2000);
        """)
    node.query("OPTIMIZE TABLE tab_bitpacking final")
    node.query("OPTIMIZE TABLE tab_uncompressed final")
    result_bitpacking = node.query(
        """
        SELECT
        sum(rows) AS `rows-count`,
        sum(bytes_on_disk) AS `total-bytes`,
        sum(secondary_indices_compressed_bytes) AS `text-index-bytes`
        FROM system.parts
        WHERE database = currentDatabase() AND active AND table = 'tab_bitpacking'
        GROUP BY `table`
        FORMAT TSV;
        """
    )
    result_uncompressed = node.query(
        """
        SELECT
        sum(rows) AS `rows-count`,
        sum(bytes_on_disk) AS `total-bytes`,
        sum(secondary_indices_compressed_bytes) AS `text-index-bytes`
        FROM system.parts
        WHERE database = currentDatabase() AND active AND table = 'tab_uncompressed'
        GROUP BY `table`
        FORMAT TSV;
        """
    )
    assert result_bitpacking == result_uncompressed
    node.query("DROP TABLE tab_bitpacking;")
    node.query("DROP TABLE tab_uncompressed;")
