#include "config.h"

#if USE_AWS_S3

#include <Storages/StorageHudi.h>
#include <Common/logger_useful.h>

#include <Formats/FormatFactory.h>
#include <Storages/StorageFactory.h>

#include <QueryPipeline/Pipe.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int LOGICAL_ERROR;
}

template <typename Configuration, typename MetadataReadHelper>
HudiMetadataParser<Configuration, MetadataReadHelper>::HudiMetadataParser(const Configuration & configuration_, ContextPtr context_)
    : configuration(configuration_), context(context_), log(&Poco::Logger::get("StorageHudi"))
{
}

/// Apache Hudi store parts of data in different files.
/// Every part file has timestamp in it.
/// Every partition(directory) in Apache Hudi has different versions of part.
/// To find needed parts we need to find out latest part file for every partition.
/// Part format is usually parquet, but can differ.
template <typename Configuration, typename MetadataReadHelper>
String HudiMetadataParser<Configuration, MetadataReadHelper>::generateQueryFromKeys(const std::vector<std::string> & keys, const String & format)
{
    /// For each partition path take only latest file.
    struct FileInfo
    {
        String filename;
        UInt64 timestamp;
    };
    std::unordered_map<String, FileInfo> latest_parts; /// Partition path (directory) -> latest part file info.

    /// Make format lowercase.
    const auto expected_extension= "." + Poco::toLower(format);
    /// Filter only files with specific format.
    auto keys_filter = [&](const String & key) { return std::filesystem::path(key).extension() == expected_extension; };

    for (const auto & key : keys | std::views::filter(keys_filter))
    {
        const auto key_path = fs::path(key);
        const String filename = key_path.filename();
        const String partition_path = key_path.parent_path();

        /// Every filename contains metadata split by "_", timestamp is after last "_".
        const auto delim = key.find_last_of('_') + 1;
        if (delim == std::string::npos)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected format of metadata files");
        const auto timestamp = parse<UInt64>(key.substr(delim + 1));

        auto it = latest_parts.find(partition_path);
        if (it == latest_parts.end())
        {
            latest_parts.emplace(partition_path, FileInfo{filename, timestamp});
        }
        else if (it->second.timestamp < timestamp)
        {
            it->second = {filename, timestamp};
        }
    }

    std::string list_of_keys;

    for (const auto & [directory, file_info] : latest_parts)
    {
        if (!list_of_keys.empty())
            list_of_keys += ",";

        list_of_keys += std::filesystem::path(directory) / file_info.filename;
    }

    return "{" + list_of_keys + "}";
}

template <typename Configuration, typename MetadataReadHelper>
std::vector<std::string> HudiMetadataParser<Configuration, MetadataReadHelper>::getFiles() const
{
    return MetadataReadHelper::listFiles(configuration);
}

template HudiMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::HudiMetadataParser(
    const StorageS3::Configuration & configuration_, ContextPtr context_);

template std::vector<String> HudiMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::getFiles() const;

template String HudiMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::generateQueryFromKeys(
    const std::vector<String> & keys, const String & format);

void registerStorageHudi(StorageFactory & factory)
{
    factory.registerStorage(
        "Hudi",
        [](const StorageFactory::Arguments & args)
        {
            StorageS3::Configuration configuration = StorageHudi::getConfiguration(args.engine_args, args.getLocalContext());

            auto format_settings = getFormatSettings(args.getContext());

            return std::make_shared<StorageHudi>(
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
