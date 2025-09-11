import pytest
import json

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    generate_data,
    get_uuid_str,
    write_iceberg_from_df
)

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

    write_iceberg_from_df(spark, generate_data(spark, 0, 100), TABLE_NAME)

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    def get_iceberg_metadata_to_dict(query_id: str):
        instance = started_cluster_iceberg_with_spark.instances["node1"]
        result = dict()
        for name in ['content', 'content_type', 'table_path', 'file_path', 'row_in_file']:
            # We are ok with duplicates in the table itself but for test purposes we want to remove duplicates here
            select_distinct_expression = f"SELECT DISTINCT(*) FROM (SELECT content, content_type, table_path, file_path, row_in_file FROM system.iceberg_metadata_log WHERE query_id = '{query_id}') ORDER BY ALL"
            query_result = instance.query(f"SELECT {name} FROM ({select_distinct_expression})")
            result[name] = query_result.split('\n')
            result[name] = list(filter(lambda x: len(x) > 0, result[name]))
        result['row_in_file'] = list(map(lambda x : int(x) if x.isdigit() else None, result['row_in_file']))
        return result
    
    def verify_result_dictionary(diction : dict, allowed_content_types : set):
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
            try:
                json.loads(content)
            except:
                raise ValueError("Content is not valid JSON. Content: {}".format(content))
        for file_path in set(diction['file_path']):
            row_values = set()
            number_of_missing_row_values = 0
            number_of_rows = 0
            for i in range(len(diction['file_path'])):
                if file_path == diction['file_path'][i]:
                    if diction['row_in_file'][i] is not None:
                        row_values.add(diction['row_in_file'][i])
                        # If row is present the type is entry
                        if diction['content_type'][i] not in ['ManifestFileEntry', 'ManifestListEntry']:
                            raise ValueError("Row should not be specified for an entry {}, file_path: {}".format(diction['content_type'][i], file_path))
                        number_of_rows += 1
                    else:
                        # If row is not present that the type is metadata
                        if diction['content_type'][i] not in ['Metadata', 'ManifestFileMetadata', 'ManifestListMetadata']:
                            raise ValueError("Row should be specified for an entry {}, file_path: {}".format(diction['content_type'][i], file_path))

                        number_of_missing_row_values += 1
                    
            # We have exactly one metadata file
            if number_of_missing_row_values != 1:
                raise ValueError("Not a one row value (corresponding to metadata file) is missing for file path: {}".format(file_path))

            # Rows in avro files are consistent
            if len(row_values) != number_of_rows:
                raise ValueError("Unexpected number of row values for file path: {}".format(file_path))
            for i in range(number_of_rows):
                if not i in row_values:
                    raise ValueError("Missing row value for file path: {}, missing row index: {}".format(file_path, i))


    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    content_types = ["Metadata", "ManifestListMetadata", "ManifestListEntry", "ManifestFileMetadata", "ManifestFileEntry"]
    settings = ["none", "metadata", "manifest_list_metadata", "manifest_list_entry", "manifest_file_metadata", "manifest_file_entry"]

    for i in range(len(settings)):
        allowed_content_types = set(content_types[:i])

        query_id = TABLE_NAME + "_" + str(i) + "_" + get_uuid_str()

        assert instance.query(f"SELECT * FROM {TABLE_NAME}", query_id = query_id,  settings={"iceberg_metadata_log_level":settings[i]})

        instance.query("SYSTEM FLUSH LOGS iceberg_metadata_log")

        diction = get_iceberg_metadata_to_dict(query_id)

        try:
            verify_result_dictionary(diction, allowed_content_types)
        except:
            print("Dictionary: {}, Allowed Content Types: {}".format(diction, allowed_content_types))
            raise
