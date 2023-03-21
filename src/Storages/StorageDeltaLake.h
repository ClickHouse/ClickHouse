#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/IStorageDataLake.h>
#include <Storages/S3DataLakeMetadataReadHelper.h>
#include <Storages/StorageS3.h>
#include <base/JSON.h>

namespace DB
{

/**
 * Documentation links:
 *  - https://github.com/delta-io/delta/blob/master/PROTOCOL.md#data-files
 */

template <typename Configuration, typename MetadataReadHelper>
class DeltaLakeMetadataParser
{
public:
    DeltaLakeMetadataParser(const Configuration & storage_configuration_, ContextPtr context)
        : storage_configuration(storage_configuration_)
    {
        processMetadataFiles(context);
    }

    Strings getFiles() { return Strings(data_files.begin(), data_files.end()); }

    static String generateQueryFromKeys(const std::vector<String> & keys, const std::string & /* format */)
    {
        if (keys.size() == 1)
        {
            return fmt::format("{}", keys[0]);
        }
        else
        {
            return fmt::format("{{{}}}", fmt::join(keys, ","));
        }
    }

private:
    /**
     * Delta files are stored as JSON in a directory at the root of the table named _delta_log,
     * and together with checkpoints make up the log of all changes that have occurred to a table.
     */
    std::vector<String> getMetadataFiles() const
    {
        /// DeltaLake format stores all metadata json files in _delta_log directory
        static constexpr auto deltalake_metadata_directory = "_delta_log";
        static constexpr auto metadata_file_suffix = ".json";

        return MetadataReadHelper::listFilesMatchSuffix(
            storage_configuration, deltalake_metadata_directory, metadata_file_suffix);
    }

    /**
     * Delta files are the unit of atomicity for a table,
     * and are named using the next available version number, zero-padded to 20 digits.
     * For example:
     *     ./_delta_log/00000000000000000000.json
     *
     * A delta file, n.json, contains an atomic set of actions that should be applied to the
     * previous table state (n-1.json) in order to the construct nth snapshot of the table.
     * An action changes one aspect of the table's state, for example, adding or removing a file.
     */
    void processMetadataFiles(ContextPtr context)
    {
        const auto keys = getMetadataFiles();
        for (const String & key : keys)
        {
            auto buf = MetadataReadHelper::createReadBuffer(key, context, storage_configuration);

            char c;
            while (!buf->eof())
            {
                /// May be some invalid characters before json.
                while (buf->peek(c) && c != '{')
                    buf->ignore();

                if (buf->eof())
                    break;

                String json_str;
                readJSONObjectPossiblyInvalid(json_str, *buf);

                if (json_str.empty())
                    continue;

                const JSON json(json_str);
                handleJSON(json);
            }
        }
    }

    /**
     * Example of content of a single .json metadata file:
     * "
     *     {"commitInfo":{
     *         "timestamp":1679424650713,
     *         "operation":"WRITE",
     *         "operationParameters":{"mode":"Overwrite","partitionBy":"[]"},
     *         "isolationLevel":"Serializable",
     *         "isBlindAppend":false,
     *         "operationMetrics":{"numFiles":"1","numOutputRows":"100","numOutputBytes":"2560"},
     *         "engineInfo":"Apache-Spark/3.3.2 Delta-Lake/2.2.0",
     *         "txnId":"8cb5814d-1009-46ad-a2f8-f1e7fdf4da56"}}
     *     {"protocol":{"minReaderVersion":2,"minWriterVersion":5}}
     *     {"metaData":{
     *         "id":"bd11ad96-bc2c-40b0-be1f-6fdd90d04459",
     *         "format":{"provider":"parquet","options":{}},
     *         "schemaString":"{
     *             \"type\":\"struct\",\"fields\":[{\"name\":\"number\",\"type\":\"decimal(20,0)\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"col-6c990940-59bb-4709-8f2e-17083a82c01a\"}},{\"name\":\"toString(number)\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"col-763cd7e2-7627-4d8e-9fb7-9e85d0c8845b\"}}]}",
     *         "partitionColumns":[],
     *         "configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"2"},
     *         "createdTime":1679424648640}}
     *     {"add":{
     *         "path":"part-00000-ecf8ed08-d04a-4a71-a5ec-57d8bb2ab4ee-c000.parquet",
     *         "partitionValues":{},
     *         "size":2560,
     *         "modificationTime":1679424649568,
     *         "dataChange":true,
     *         "stats":"{
     *             \"numRecords\":100,
     *             \"minValues\":{\"col-6c990940-59bb-4709-8f2e-17083a82c01a\":0},
     *             \"maxValues\":{\"col-6c990940-59bb-4709-8f2e-17083a82c01a\":99},
     *             \"nullCount\":{\"col-6c990940-59bb-4709-8f2e-17083a82c01a\":0,\"col-763cd7e2-7627-4d8e-9fb7-9e85d0c8845b\":0}}"}}
     * "
     */
    void handleJSON(const JSON & json)
    {
        if (json.has("add"))
        {
            const auto path = json["add"]["path"].getString();
            const auto [_, inserted] = data_files.insert(path);
            if (!inserted)
                throw Exception(ErrorCodes::INCORRECT_DATA, "File already exists {}", path);
        }
        else if (json.has("remove"))
        {
            const auto path = json["remove"]["path"].getString();
            const bool erase = data_files.erase(path);
            if (!erase)
                throw Exception(ErrorCodes::INCORRECT_DATA, "File doesn't exist {}", path);
        }
    }

    Configuration storage_configuration;
    std::set<String> data_files;
};

struct StorageDeltaLakeName
{
    static constexpr auto name = "DeltaLake";
};

using StorageDeltaLake
    = IStorageDataLake<StorageS3, StorageDeltaLakeName, DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>>;
}

#endif
