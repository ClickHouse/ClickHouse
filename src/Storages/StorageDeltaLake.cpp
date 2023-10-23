#include "config.h"
#if USE_AWS_S3

#include <Storages/StorageDeltaLake.h>
#include <Common/logger_useful.h>

#include <Storages/StorageFactory.h>

#include <Formats/FormatFactory.h>

#include <QueryPipeline/Pipe.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int INCORRECT_DATA;
}

void DeltaLakeMetadata::setLastModifiedTime(const String & filename, uint64_t timestamp)
{
    file_update_time[filename] = timestamp;
}

void DeltaLakeMetadata::remove(const String & filename, uint64_t /*timestamp */)
{
    bool erase = file_update_time.erase(filename);
    if (!erase)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid table metadata, tried to remove {} before adding it", filename);
}

std::vector<String> DeltaLakeMetadata::listCurrentFiles() &&
{
    std::vector<String> keys;
    keys.reserve(file_update_time.size());

    for (auto && [k, _] : file_update_time)
        keys.push_back(k);

    return keys;
}

template <typename Configuration, typename MetadataReadHelper>
DeltaLakeMetadataParser<Configuration, MetadataReadHelper>::DeltaLakeMetadataParser(const Configuration & configuration_, ContextPtr context)
    : base_configuration(configuration_)
{
    init(context);
}

template <typename Configuration, typename MetadataReadHelper>
void DeltaLakeMetadataParser<Configuration, MetadataReadHelper>::init(ContextPtr context)
{
    auto keys = getJsonLogFiles();

    // read data from every json log file
    for (const String & key : keys)
    {
        auto buf = MetadataReadHelper::createReadBuffer(key, context, base_configuration);

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

template <typename Configuration, typename MetadataReadHelper>
std::vector<String> DeltaLakeMetadataParser<Configuration, MetadataReadHelper>::getJsonLogFiles() const
{

    /// DeltaLake format stores all metadata json files in _delta_log directory
    static constexpr auto deltalake_metadata_directory = "_delta_log";
    static constexpr auto meta_file_suffix = ".json";

    return MetadataReadHelper::listFilesMatchSuffix(base_configuration, deltalake_metadata_directory, meta_file_suffix);
}

template <typename Configuration, typename MetadataReadHelper>
void DeltaLakeMetadataParser<Configuration, MetadataReadHelper>::handleJSON(const JSON & json)
{
    if (json.has("add"))
    {
        auto path = json["add"]["path"].getString();
        auto timestamp = json["add"]["modificationTime"].getInt();

        metadata.setLastModifiedTime(path, timestamp);
    }
    else if (json.has("remove"))
    {
        auto path = json["remove"]["path"].getString();
        auto timestamp = json["remove"]["deletionTimestamp"].getInt();

        metadata.remove(path, timestamp);
    }
}


// DeltaLake stores data in parts in different files
// keys is vector of parts with latest version
// generateQueryFromKeys constructs query from parts filenames for
// underlying StorageS3 engine
template <typename Configuration, typename MetadataReadHelper>
String DeltaLakeMetadataParser<Configuration, MetadataReadHelper>::generateQueryFromKeys(const std::vector<String> & keys, const String &)
{
    std::string new_query = fmt::format("{{{}}}", fmt::join(keys, ","));
    return new_query;
}

template DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::DeltaLakeMetadataParser(
    const StorageS3::Configuration & configuration_, ContextPtr context);

template std::vector<String> DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::getFiles();

template String DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::generateQueryFromKeys(
    const std::vector<String> & keys, const String & format);

template void DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::init(ContextPtr context);

template std::vector<String> DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::getJsonLogFiles() const;

template void DeltaLakeMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::handleJSON(const JSON & json);

void registerStorageDeltaLake(StorageFactory & factory)
{
    factory.registerStorage(
        "DeltaLake",
        [](const StorageFactory::Arguments & args)
        {
            StorageS3::Configuration configuration = StorageDeltaLake::getConfiguration(args.engine_args, args.getLocalContext());

            auto format_settings = getFormatSettings(args.getContext());

            return std::make_shared<StorageDeltaLake>(
                configuration, args.table_id, args.columns, args.constraints, args.comment, args.getContext(), format_settings);
        },
        {
            .supports_settings = false,
            .supports_schema_inference = true,
            .source_access_type = AccessType::S3,
        });
}

}

#endif
