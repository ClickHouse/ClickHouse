from datetime import datetime, timedelta
import pytest
import json

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    get_uuid_str,
    execute_spark_query_general
)

class PrunedInfo:
    def __init__(self, not_pruned, partition_pruned, min_max_index_pruned):
        self.not_pruned = not_pruned
        self.partition_pruned = partition_pruned
        self.min_max_index_pruned = min_max_index_pruned

    def __repr__(self):
        return "PrunedInfo(not_pruned={}, partition_pruned={}, min_max_index_pruned={})".format(self.not_pruned, self.partition_pruned, self.min_max_index_pruned)
    
    def __eq__(self, other):
        return (self.not_pruned == other.not_pruned and
                self.partition_pruned == other.partition_pruned and
                self.min_max_index_pruned == other.min_max_index_pruned)

def get_date_and_time_columns(instance, query_id: str):
    result = dict()
    for name in ['event_date', 'event_time']:
        query_result = instance.query(f"SELECT {name} FROM system.iceberg_metadata_log WHERE query_id = '{query_id}'")
        result[name] = query_result.split('\n')
        result[name] = list(filter(lambda x: len(x) > 0, result[name]))
    return result

def get_iceberg_metadata_to_dict(instance, query_id: str):
    result = dict()
    for name in ['content', 'content_type', 'table_path', 'file_path', 'row_in_file', 'pruning_status']:
        # We are ok with duplicates in the table itself but for test purposes we want to remove duplicates here
        select_distinct_expression = f"SELECT DISTINCT(*) FROM (SELECT content, content_type, table_path, file_path, row_in_file, pruning_status FROM system.iceberg_metadata_log WHERE query_id = '{query_id}') ORDER BY ALL"
        query_result = instance.query(f"SELECT {name} FROM ({select_distinct_expression})")
        print("Query result for {}: {}".format(name, query_result))
        result[name] = query_result.split('\n')[:-1]
    result['row_in_file'] = list(map(lambda x : int(x) if x.isdigit() else None, result['row_in_file']))
    result['pruning_status'] = list(map(lambda x : x if x != '\\N' else None, result['pruning_status']))
    print("Result dictionary: {}".format(result))
    return result

def verify_result_dictionary(diction : dict, allowed_content_types : set):
    prunned_info = PrunedInfo(0, 0, 0)
    # Expected content_type and only it is present
    if set(diction['content_type']) != allowed_content_types:
        raise ValueError("Content type mismatch. Expected: {}, got: {}".format(allowed_content_types, set(diction['content_type'])))
    # For all entries we have the same table_path
    if not (len(set(diction['table_path'])) == 1 or (len(allowed_content_types) == 0 and len(diction['table_path']) == 0)):
        raise ValueError("Unexpected number of table paths are found for one query. Set: {}".format(set(diction['table_path'])))
    extensions = list(map(lambda x: x.split('.')[-1], diction['file_path']))
    for i in range(len(diction['content_type'])):
        if diction['content_type'][i] == 'Metadata':
            # File with content_type 'Metadata' has json extension
            if extensions[i] != 'json':
                raise ValueError("Unexpected file extension for Metadata. Expected: json, got: {}".format(extensions[i]))
        else:
            # File with content_types except 'Metadata' has avro extension
            if extensions[i] != 'avro':
                raise ValueError("Unexpected file extension for {}. Expected: avro, got: {}".format(diction['content_type'][i], extensions[i]))

    # All content is json-serializable
    for content in diction['content']:
        if content == '':
            continue
        try:
            json.loads(content)
        except:
            raise ValueError("Content is not valid JSON. Content: {}".format(content))
    for file_path in set(diction['file_path']):
        row_values = set()
        number_of_missing_row_values = 0
        number_of_rows = 0
        partitioned_rows = set()
        not_deleted_files = set()
        for i in range(len(diction['file_path'])):
            if file_path == diction['file_path'][i]:
                if diction['row_in_file'][i] is not None:
                    row_values.add(diction['row_in_file'][i])
                    # If row is present the type is entry
                    if diction['content_type'][i] not in ['ManifestFileEntry', 'ManifestListEntry']:
                        raise ValueError("Row should not be specified for an entry {}, file_path: {}".format(diction['content_type'][i], file_path))
                    if diction['content'][i] != '':
                        number_of_rows += 1

                    if diction['content_type'][i] == 'ManifestFileEntry':
                        if diction['content'][i] == '':
                            if diction['pruning_status'][i] is None:
                                raise ValueError("Pruning status should be specified for this manifest file entry, file_path: {}".format(file_path))
                            partitioned_rows.add(diction['row_in_file'][i])
                            if diction['pruning_status'][i] == 'NotPruned':
                                prunned_info.not_pruned += 1
                            elif diction['pruning_status'][i] == 'PartitionPruned':
                                prunned_info.partition_pruned += 1
                            elif diction['pruning_status'][i] == 'MinMaxIndexPruned':
                                prunned_info.min_max_index_pruned += 1
                            else:
                                raise ValueError("Unexpected pruning status: {}, file_path: {}".format(diction['pruning_status'][i], file_path))
                        else:
                            data_object = json.loads(diction['content'][i])
                            print("Data object: {}".format(data_object))
                            if data_object['status'] < 2:
                                not_deleted_files.add(diction['row_in_file'][i])
                else:
                    # If row is not present that the type is metadata
                    if diction['content_type'][i] not in ['Metadata', 'ManifestFileMetadata', 'ManifestListMetadata']:
                        raise ValueError("Row should be specified for an entry {}, file_path: {}".format(diction['content_type'][i], file_path))

                    number_of_missing_row_values += 1
        if partitioned_rows != not_deleted_files:
            raise ValueError("Partitioned rows are not consistent with not deleted files for file path: {}, partitioned rows: {}, not deleted files: {}".format(file_path, partitioned_rows, not_deleted_files))
                
        # We have exactly one metadata file
        if number_of_missing_row_values != 1:
            raise ValueError("Not a one row value (corresponding to metadata file) is missing for file path: {}".format(file_path))

        # Rows in avro files are consistent
        if len(row_values) != number_of_rows:
            raise ValueError("Unexpected number of row values for file path: {}".format(file_path))
        for i in range(number_of_rows):
            if not i in row_values:
                raise ValueError("Missing row value for file path: {}, missing row index: {}".format(file_path, i))
    return prunned_info

def get_prunned_info_from_profile_events(instance, query_id: str):
    instance.query("SYSTEM FLUSH LOGS")

    not_pruned = int(
        instance.query(
            f"SELECT ProfileEvents['IcebergMetadataReturnedObjectInfos'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    partition_pruned = int(
        instance.query(
            f"SELECT ProfileEvents['IcebergPartitionPrunedFiles'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    min_max_index_pruned = int(
        instance.query(
            f"SELECT ProfileEvents['IcebergMinMaxIndexPrunedFiles'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    return PrunedInfo(not_pruned, partition_pruned, min_max_index_pruned)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
def test_system_iceberg_metadata(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_system_iceberg_metadata_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster_iceberg_with_spark,
            storage_type,
            TABLE_NAME,
            query,
        )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                a INT,
                b STRING
            )
            USING iceberg
            PARTITIONED BY (identity(a))
            OPTIONS('format-version'='2')
        """
    )

    for i in range(5):
        spark.sql(
            f"""
                INSERT INTO {TABLE_NAME} VALUES
                ({i}, '{i}');
            """
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    content_types = ["Metadata", "ManifestListMetadata", "ManifestListEntry", "ManifestFileMetadata", "ManifestFileEntry"]
    settings = ["none", "metadata", "manifest_list_metadata", "manifest_list_entry", "manifest_file_metadata", "manifest_file_entry"]


    for i in range(len(settings)):
        allowed_content_types = set(content_types[:i])

        query_id = TABLE_NAME + "_" + str(i) + "_" + get_uuid_str()

        instance.query(f"SELECT * FROM {TABLE_NAME} WHERE a >= 2", query_id = query_id,  settings={"iceberg_metadata_log_level":settings[i], "use_iceberg_partition_pruning": 1})

        expected_prunned_info = get_prunned_info_from_profile_events(instance, query_id)

        diction = get_iceberg_metadata_to_dict(instance, query_id)

        try:
            if settings[i] == 'manifest_file_entry':
                table_prunned_info = verify_result_dictionary(diction, allowed_content_types)
                assert table_prunned_info == expected_prunned_info, "Not prunned files count mismatch. Table: {}, ProfileEvents: {}".format(table_prunned_info, expected_prunned_info)
        except:
            print("Dictionary: {}, Allowed Content Types: {}".format(diction, allowed_content_types))
            raise

        date_and_time_columns = get_date_and_time_columns(instance, query_id)
        
        event_dates = date_and_time_columns['event_date']
        event_times = date_and_time_columns['event_time']

        for date_str in event_dates:
            current_date = datetime.fromisoformat(date_str)
            assert current_date.date() <= datetime.now().date(), "Event date is in the future. Event date: {}, current date: {}".format(current_date.date(), datetime.now().date())
            assert current_date.date() >= (datetime.now().date() - timedelta(days=2)), "Event date is too old. Event date: {}, current date: {}".format(current_date.date(), datetime.now().date())

        for time_str in event_times:
            current_time = datetime.fromisoformat(time_str)
            assert current_time <= datetime.now(), "Event time is in the future. Event time: {}, current time: {}".format(current_time, datetime.now())
            assert current_time >= (datetime.now() - timedelta(days=1)), "Event time is too old. Event time: {}, current time: {}".format(current_time, datetime.now())