#include <Storages/DataLakes/DeltaLakeMetadataParser.h>
#include <base/JSON.h>
#include "config.h"
#include <set>

#if USE_AWS_S3
#include <Storages/DataLakes/S3MetadataReader.h>
#include <Storages/StorageS3.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{
    /**
     * Useful links:
     *  - https://github.com/delta-io/delta/blob/master/PROTOCOL.md#data-files
     */

    /**
     * DeltaLake tables store metadata files and data files.
     * Metadata files are stored as JSON in a directory at the root of the table named _delta_log,
     * and together with checkpoints make up the log of all changes that have occurred to a table.
     *
     * Delta files are the unit of atomicity for a table,
     * and are named using the next available version number, zero-padded to 20 digits.
     * For example:
     *     ./_delta_log/00000000000000000000.json
     */
    template <typename Configuration, typename MetadataReadHelper>
    std::vector<String> getMetadataFiles(const Configuration & configuration)
    {
        /// DeltaLake format stores all metadata json files in _delta_log directory
        static constexpr auto deltalake_metadata_directory = "_delta_log";
        static constexpr auto metadata_file_suffix = ".json";

        return MetadataReadHelper::listFiles(configuration, deltalake_metadata_directory, metadata_file_suffix);
    }

    /**
     * Example of content of a single .json metadata file:
     * "
     *     {"commitInfo":{
     *         "timestamp":1679424650713,
     *         "operation":"WRITE",
     *         "operationMetrics":{"numFiles":"1","numOutputRows":"100","numOutputBytes":"2560"},
     *         ...}
     *     {"protocol":{"minReaderVersion":2,"minWriterVersion":5}}
     *     {"metaData":{
     *         "id":"bd11ad96-bc2c-40b0-be1f-6fdd90d04459",
     *         "format":{"provider":"parquet","options":{}},
     *         "schemaString":"{...}",
     *         "partitionColumns":[],
     *         "configuration":{...},
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
    void handleJSON(const JSON & json, std::set<String> & result)
    {
        if (json.has("add"))
        {
            const auto path = json["add"]["path"].getString();
            const auto [_, inserted] = result.insert(path);
            if (!inserted)
                throw Exception(ErrorCodes::INCORRECT_DATA, "File already exists {}", path);
        }
        else if (json.has("remove"))
        {
            const auto path = json["remove"]["path"].getString();
            const bool erase = result.erase(path);
            if (!erase)
                throw Exception(ErrorCodes::INCORRECT_DATA, "File doesn't exist {}", path);
        }
    }

    /**
     * A delta file, n.json, contains an atomic set of actions that should be applied to the
     * previous table state (n-1.json) in order to the construct nth snapshot of the table.
     * An action changes one aspect of the table's state, for example, adding or removing a file.
     */
    template <typename Configuration, typename MetadataReadHelper>
    std::set<String> processMetadataFiles(const Configuration & configuration, ContextPtr context)
    {
        std::set<String> result;
        const auto keys = getMetadataFiles<Configuration, MetadataReadHelper>(configuration);
        for (const String & key : keys)
        {
            auto buf = MetadataReadHelper::createReadBuffer(key, context, configuration);

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
                handleJSON(json, result);
            }
        }
        return result;
    }
}

template <typename Configuration, typename MetadataReadHelper>
Strings DeltaLakeMetadataParser<Configuration, MetadataReadHelper>::getFiles(const Configuration & configuration, ContextPtr context)
{
    auto data_files = processMetadataFiles<Configuration, MetadataReadHelper>(configuration, context);
    return Strings(data_files.begin(), data_files.end());
}

template Strings DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::getFiles(
    const StorageS3::Configuration & configuration, ContextPtr);
}

#endif
