#pragma once

#if USE_AWS_S3 && USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

#include <Storages/StorageS3.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>

namespace DB
{

/**
 * Useful links:
 * - https://iceberg.apache.org/spec/
 *
 * Iceberg has two format versions, v1 and v2. The content of metadata files depends on the version.
 *
 * Unlike DeltaLake, Iceberg has several metadata layers: `table metadata`, `manifest list` and `manifest_files`.
 * Metadata file - json file.
 * Manifest list – an Avro file that lists manifest files; one per snapshot.
 * Manifest file – an Avro file that lists data or delete files; a subset of a snapshot.
 * All changes to table state create a new metadata file and replace the old metadata with an atomic swap.
 *
 * In order to find out which data files to read, we need to find the `manifest list`
 * which corresponds to the latest snapshot. We find it by checking a list of snapshots
 * in metadata's "snapshots" section.
 *
 * Example of metadata.json file.
 * {
 *     "format-version" : 1,
 *     "table-uuid" : "ca2965ad-aae2-4813-8cf7-2c394e0c10f5",
 *     "location" : "/iceberg_data/db/table_name",
 *     "last-updated-ms" : 1680206743150,
 *     "last-column-id" : 2,
 *     "schema" : { "type" : "struct", "schema-id" : 0, "fields" : [ {<field1_info>}, {<field2_info>}, ... ] },
 *     "current-schema-id" : 0,
 *     "schemas" : [ ],
 *     ...
 *     "current-snapshot-id" : 2819310504515118887,
 *     "refs" : { "main" : { "snapshot-id" : 2819310504515118887, "type" : "branch" } },
 *     "snapshots" : [ {
 *       "snapshot-id" : 2819310504515118887,
 *       "timestamp-ms" : 1680206743150,
 *       "summary" : {
 *         "operation" : "append", "spark.app.id" : "local-1680206733239",
 *         "added-data-files" : "1", "added-records" : "100",
 *         "added-files-size" : "1070", "changed-partition-count" : "1",
 *         "total-records" : "100", "total-files-size" : "1070", "total-data-files" : "1", "total-delete-files" : "0",
 *         "total-position-deletes" : "0", "total-equality-deletes" : "0"
 *       },
 *       "manifest-list" : "/iceberg_data/db/table_name/metadata/snap-2819310504515118887-1-c87bfec7-d36c-4075-ad04-600b6b0f2020.avro",
 *       "schema-id" : 0
 *     } ],
 *     "statistics" : [ ],
 *     "snapshot-log" : [ ... ],
 *     "metadata-log" : [ ]
 * }
 */
class IcebergMetadata : WithContext
{
public:
    IcebergMetadata(const StorageS3::Configuration & configuration_,
                    ContextPtr context_,
                    Int32 metadata_version_,
                    Int32 format_version_,
                    String manifest_list_file_,
                    Int32 current_schema_id_,
                    NamesAndTypesList schema_);

    /// Get data files. On first request it reads manifest_list file and iterates through manifest files to find all data files.
    /// All subsequent calls will return saved list of files (because it cannot be changed without changing metadata file)
    Strings getDataFiles();

    /// Get table schema parsed from metadata.
    NamesAndTypesList getTableSchema() const { return schema; }

    size_t getVersion() const { return metadata_version; }

private:
    const StorageS3::Configuration configuration;
    Int32 metadata_version;
    Int32 format_version;
    String manifest_list_file;
    Int32 current_schema_id;
    NamesAndTypesList schema;
    Strings data_files;
    LoggerPtr log;

};

std::unique_ptr<IcebergMetadata> parseIcebergMetadata(const StorageS3::Configuration & configuration, ContextPtr context);

}

#endif
