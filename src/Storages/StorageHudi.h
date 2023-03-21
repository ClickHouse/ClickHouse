#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/IStorage.h>
#include <Storages/IStorageDataLake.h>
#include <Storages/S3DataLakeMetadataReadHelper.h>
#include <Storages/StorageS3.h>
#include <Common/logger_useful.h>
#include <ranges>


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

template <typename Configuration, typename MetadataReadHelper>
class HudiMetadataParser
{
public:
    HudiMetadataParser(const Configuration & configuration_, ContextPtr context_)
        : configuration(configuration_), context(context_), log(&Poco::Logger::get("StorageHudi")) {}

    Strings getFiles() const { return MetadataReadHelper::listFiles(configuration); }

    /** Apache Hudi store parts of data in different files.
      * Every part file has timestamp in it.
      * Every partition(directory) in Apache Hudi has different versions of part.
      * To find needed parts we need to find out latest part file for every partition.
      * Part format is usually parquet, but can differ.
      */
    static String generateQueryFromKeys(const std::vector<String> & keys, const String & format)
    {
        auto * log = &Poco::Logger::get("HudiMetadataParser");

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

        /// For each partition path take only latest file.
        for (const auto & key : keys | std::views::filter(keys_filter))
        {
            const auto key_path = fs::path(key);

            /// Every filename contains metadata split by "_", timestamp is after last "_".
            const auto delim = key.find_last_of('_') + 1;
            if (delim == std::string::npos)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected format of metadata files");

            const auto timestamp = parse<UInt64>(key.substr(delim + 1));

            const auto [it, inserted] = latest_parts.emplace(/* partition_path */key_path.parent_path(), FileInfo{});
            if (inserted)
            {
                it->second = FileInfo{key_path.filename(), timestamp};
            }
            else if (it->second.timestamp < timestamp)
            {
                it->second = {key_path.filename(), timestamp};
            }
        }

        LOG_TRACE(log, "Having {} result partitions", latest_parts.size());

        std::string list_of_keys;
        for (const auto & [directory, file_info] : latest_parts)
        {
            if (!list_of_keys.empty())
                list_of_keys += ",";

            LOG_TEST(log, "Partition: {}, file: {}, timestamp: {}", directory, file_info.filename, file_info.timestamp);
            list_of_keys += file_info.filename;
        }

        if (latest_parts.size() == 1)
            return list_of_keys;

        return "{" + list_of_keys + "}";
    }

private:
    Configuration configuration;
    ContextPtr context;
    Poco::Logger * log;
};

struct StorageHudiName
{
    static constexpr auto name = "Hudi";
};

using StorageHudi
    = IStorageDataLake<StorageS3, StorageHudiName, HudiMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>>;
}

#endif
