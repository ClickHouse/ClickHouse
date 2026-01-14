import json
import re
import pytest
from typing import Optional, Any

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    create_iceberg_table,
    get_creation_expression
)


def get_array(query_result: str):
    arr = sorted([int(x) for x in query_result.strip().split("\n")])
    print(arr)
    return arr

@pytest.mark.parametrize("run_on_cluster", [False, True])
@pytest.mark.parametrize("use_roaring_bitmaps", [0, 1])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_position_deletes(started_cluster_iceberg_with_spark, use_roaring_bitmaps,  storage_type, run_on_cluster):
    if storage_type == "local" and run_on_cluster:
        pytest.skip("Local storage with cluster execution is not supported")
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_position_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg PARTITIONED BY (bucket(5, id)) TBLPROPERTIES ('format-version' = '2', 'write.update.mode'=
        'merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 100)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    expression = get_creation_expression(storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, run_on_cluster=run_on_cluster, table_function=True)

    print(expression)

    settings = {"use_roaring_bitmap_iceberg_positional_deletes": use_roaring_bitmaps}
    assert int(instance.query(f"SELECT count() FROM {expression}", settings=settings)) == 90

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert get_array(instance.query(f"SELECT id FROM {expression}", settings=settings)) == list(range(20, 100))

    # Check that filters are applied after deletes
    assert int(instance.query(f"SELECT count() FROM {expression} where id >= 15", settings=settings)) == 80
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {expression} where id >= 15 SETTINGS optimize_trivial_count_query=1",
                settings=settings,
            )
        )
        == 80
    )

    # Check deletes after deletes
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id >= 90")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    assert get_array(instance.query(f"SELECT id FROM {expression}", settings=settings)) == list(range(20, 90))

    spark.sql(f"ALTER TABLE {TABLE_NAME} ADD PARTITION FIELD truncate(1, data)")

    # Check adds after deletes
    spark.sql(
        f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 200)"
    )
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    assert get_array(instance.query(f"SELECT id FROM {expression}", settings=settings)) == list(range(20, 90)) + list(
        range(100, 200)
    )

    # Check deletes after adds
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id >= 150")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    assert get_array(instance.query(f"SELECT id FROM {expression}", settings=settings)) == list(range(20, 90)) + list(
        range(100, 150)
    )

    assert get_array(
        instance.query(
            f"SELECT id FROM {expression} WHERE id = 70 SETTINGS use_iceberg_partition_pruning = 1",
            settings=settings,
        )
    ) == [70]

    # Check PREWHERE (or regular WHERE if optimize_move_to_prewhere = 0 or
    # input_format_parquet_use_native_reader_v3 = 0)
    assert get_array(
        instance.query(
            f"SELECT id FROM {expression} WHERE id % 3 = 0", settings=settings)) == list(range(21, 90, 3)) + list(range(102, 150, 3))

@pytest.mark.parametrize("use_roaring_bitmaps", [0, 1])
def test_position_deletes_out_of_order(started_cluster_iceberg_with_spark, use_roaring_bitmaps):
    storage_type = "local"
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_position_deletes_out_of_order_" + get_uuid_str()

    settings = {
        "use_roaring_bitmap_iceberg_positional_deletes": use_roaring_bitmaps,
        "input_format_parquet_use_native_reader_v3": 1,
    }

    # There are a few flaky hacks chained together here.
    # We want the parquet reader to produce chunks corresponding to row groups out of order if
    # `format_settings.parquet.preserve_order` wasn't enabled. For that we:
    #  * Use `PREWHERE NOT sleepEachRow(...)` to make the reader take longer for bigger row groups.
    #  * Set spark row group size limit to 1 byte. Rely on current spark implementation detail:
    #    it'll check this limit every 100 rows. So effectively we've set row group size to 100 rows.
    #  * Insert 105 rows. So the first row group will have 100 rows, the second 5 rows.
    # If one of these steps breaks in future, this test will be less effective but won't fail.

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read', 'write.parquet.row-group-size-bytes'='1')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select /*+ COALESCE(1) */ id, char(id + ascii('a')) from range(0, 105)")
    # (Fun fact: if you replace these two queries with one query with `WHERE id < 10 OR id = 103`,
    #  spark either quetly fails to delete row 103 or outright crashes with segfault in jre.)
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 10")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id = 103")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, additional_settings=list(map(lambda kv: f'{kv[0]}={kv[1]}', settings.items())))

    # TODO: Replace WHERE with PREWHERE when we add prewhere support for datalakes.
    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} WHERE NOT sleepEachRow(1/100) order by id", settings=settings)) == list(range(10, 103)) + [104]

    instance.query(f"DROP TABLE {TABLE_NAME}")


class LogEntry:
    def __init__(self, position_delete_file: str, data_file: str, 
                 lower_bound: Optional[str], upper_bound: Optional[str], action: str):
        self.position_delete_file = position_delete_file
        self.data_file = data_file
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.action = action


class ManifestEntry:
    def __init__(self, file_path: str, is_delete: bool, is_data: bool,
                 lower_bound: Optional[str], upper_bound: Optional[str]):
        self.file_path = file_path
        self.is_delete = is_delete
        self.is_data = is_data
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound


def parse_log_entry(log_line: str) -> Optional[LogEntry]:
    """
    Parse a log line to extract data_file, position_delete_file, lower_bound, upper_bound.
    """
    skipping_pattern = r"Skipping position delete file `([^`]+)` for data file `([^`]+)`.*\(lower bound: `([^`]+)`, upper bound: `([^`]+)`\)"
    processing_pattern = r"Processing position delete file `([^`]+)` for data file `([^`]+)` with reference data file bounds:.*\(lower bound: `([^`]+)`, upper bound: `([^`]+)`\)"
    
    match = re.search(skipping_pattern, log_line)
    if match:
        return LogEntry(
            position_delete_file=match.group(1),
            data_file=match.group(2),
            lower_bound=match.group(3) if match.group(3) != '[no lower bound]' else None,
            upper_bound=match.group(4) if match.group(4) != '[no upper bound]' else None,
            action='skipping'
        )
    
    match = re.search(processing_pattern, log_line)
    if match:
        return LogEntry(
            position_delete_file=match.group(1),
            data_file=match.group(2),
            lower_bound=match.group(3) if match.group(3) != '[no lower bound]' else None,
            upper_bound=match.group(4) if match.group(4) != '[no upper bound]' else None,
            action='processing'
        )
    
    assert False, "Failed to parse log entry: " + log_line


def unescape_path(path: Optional[str]) -> Optional[str]:
    """
    Unescape a path that has backslash-escaped forward slashes.
    """
    if path is None:
        return None
    return path.replace('\\/', '/')


def get_bound_for_column(bounds: Any, column_id: int) -> Optional[str]:
    """
    Extract bound value for a specific column ID from bounds structure.
    Bounds can be either a dict {column_id: value} or a list of {key: column_id, value: value}.
    Returns unescaped path.
    """
    if bounds is None:
        return None
    value: Optional[str] = None
    if isinstance(bounds, dict):
        value = bounds.get(str(column_id))
    elif isinstance(bounds, list):
        for item in bounds:
            if isinstance(item, dict) and item.get('key') == column_id:
                value = item.get('value')
                break
    return unescape_path(value)


def parse_manifest_entry(content_json: dict[str, Any]) -> ManifestEntry:
    """
    Parse manifest file entry JSON to extract file info and bounds for column 2147483546.
    """
    DATA_FILE_COLUMN_ID = 2147483546
    
    data_file = content_json.get('data_file', {})
    file_path: str = data_file.get('file_path', '')
    content_type: int = data_file.get('content', 0)  # 0 = data, 1 = position delete, 2 = equality delete
    
    lower_bounds = data_file.get('lower_bounds')
    upper_bounds = data_file.get('upper_bounds')
    
    lower_bound = get_bound_for_column(lower_bounds, DATA_FILE_COLUMN_ID)
    upper_bound = get_bound_for_column(upper_bounds, DATA_FILE_COLUMN_ID)
    
    return ManifestEntry(
        file_path=file_path,
        is_delete=(content_type == 1),  # position delete
        is_data=(content_type == 0),
        lower_bound=lower_bound,
        upper_bound=upper_bound
    )


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_position_deletes_bounds_logging(started_cluster_iceberg_with_spark, storage_type: str) -> None:
    """
    Test that verifies position delete bounds filtering logs match manifest file entries.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_position_deletes_bounds_logging_" + get_uuid_str()

    # Part 1: Create unpartitioned Iceberg table with position deletes mode.
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode'='merge-on-read',
            'write.delete.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read'
        )
        """
    )

    # Part 2: Insert data in 3 separate batches to create different data files.
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(0, 100)")
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 200)")
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(200, 300)")

    # Part 3: Delete rows from each data file to create position delete files with bounds.
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id >= 50 AND id < 60")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id >= 150 AND id < 160")
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id >= 250 AND id < 260")

    # Part 4: Upload data to storage and create ClickHouse table.
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    # Part 5: Query table and verify correct data is returned after applying deletes.
    expected_ids = list(range(0, 50)) + list(range(60, 150)) + list(range(160, 250)) + list(range(260, 300))
    assert get_array(instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_metadata_log_level = 'manifest_file_entry'")) == expected_ids

    # Part 6: Parse position delete logs to extract bounds info.
    skipping_logs = instance.grep_in_log("Skipping position delete file")
    processing_logs = instance.grep_in_log("Processing position delete file")

    parsed_log_entries: list[LogEntry] = []
    has_skipping_logs: bool = False
    has_processing_logs: bool = False
    for log_line in (skipping_logs or '').splitlines():
        entry = parse_log_entry(log_line)
        if entry:
            parsed_log_entries.append(entry)
            has_skipping_logs = True
    for log_line in (processing_logs or '').splitlines():
        entry = parse_log_entry(log_line)
        if entry:
            parsed_log_entries.append(entry)
            has_processing_logs = True

    # Verify we have both skipping and processing logs (bounds filtering is working).
    assert has_skipping_logs
    assert has_processing_logs

    # Part 7: Parse manifest file entries from system.iceberg_metadata_log.
    instance.query("SYSTEM FLUSH LOGS")
    metadata_log_query = """
        SELECT DISTINCT content
        FROM system.iceberg_metadata_log 
        WHERE content != '' AND content IS NOT NULL AND content_type = 'ManifestFileEntry'
    """
    metadata_log_result = instance.query(metadata_log_query)

    delete_files_from_manifest: dict[str, ManifestEntry] = {}
    data_files_from_manifest: set[str] = set()
    for line in metadata_log_result.strip().split('\n'):
        if line:
            try:
                content_json = json.loads(line)
                entry = parse_manifest_entry(content_json)
                if entry.is_delete:
                    file_name = entry.file_path.split('/')[-1]
                    delete_files_from_manifest[file_name] = entry
                elif entry.is_data:
                    file_name = entry.file_path.split('/')[-1]
                    data_files_from_manifest.add(file_name)
            except json.JSONDecodeError:
                pass

    # Part 8: Verify that bounds from logs match bounds from manifest entries.
    for log_entry in parsed_log_entries:
        delete_file_name = log_entry.position_delete_file.split('/')[-1]
        
        assert delete_file_name in delete_files_from_manifest, \
            f"Delete file {delete_file_name} from logs not found in manifest entries"
        
        manifest_entry = delete_files_from_manifest[delete_file_name]
        
        assert log_entry.lower_bound == manifest_entry.lower_bound, \
            f"Lower bound mismatch for {delete_file_name}: log={log_entry.lower_bound}, manifest={manifest_entry.lower_bound}"
        assert log_entry.upper_bound == manifest_entry.upper_bound, \
            f"Upper bound mismatch for {delete_file_name}: log={log_entry.upper_bound}, manifest={manifest_entry.upper_bound}"

        # Part 9: Verify skip/process decision matches bounds check logic.
        data_file_path = log_entry.data_file
        lower = manifest_entry.lower_bound
        upper = manifest_entry.upper_bound
        is_within_bounds = (lower is None or lower <= data_file_path) and (upper is None or upper >= data_file_path)
        
        if log_entry.action == 'skipping':
            assert not is_within_bounds, \
                f"Data file {data_file_path} was skipped but is within bounds [{lower}, {upper}]"
        else:
            assert is_within_bounds, \
                f"Data file {data_file_path} was processed but is NOT within bounds [{lower}, {upper}]"
