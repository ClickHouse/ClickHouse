#include <Storages/DataLakes/HudiMetadataParser.h>
#include <Common/logger_useful.h>
#include <ranges>
#include <Poco/String.h>
#include "config.h"
#include <filesystem>
#include <IO/ReadHelpers.h>

#if USE_AWS_S3
#include <Storages/DataLakes/S3MetadataReader.h>
#include <Storages/StorageS3.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    /**
     * Documentation links:
     * - https://hudi.apache.org/tech-specs/
     */

    /**
      * Hudi tables store metadata files and data files.
      * Metadata files are stored in .hoodie/metadata directory.
      * There can be two types of data files
      * 1. base files (columnar file formats like Apache Parquet/Orc)
      * 2. log files
      * Currently we support reading only `base files`.
      * Data file name format:
      * [File Id]_[File Write Token]_[Transaction timestamp].[File Extension]
      *
      * To find needed parts we need to find out latest part file for every partition.
      * Part format is usually parquet, but can differ.
      */
    Strings processMetadataFiles(const std::vector<String> & keys, const String & format)
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
            const auto key_path = std::filesystem::path(key);

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

        Strings result;
        for (const auto & [_, file_info] : latest_parts)
            result.push_back(file_info.filename);
        return result;
    }
}

template <typename Configuration, typename MetadataReadHelper>
Strings HudiMetadataParser<Configuration, MetadataReadHelper>::getFiles(const Configuration & configuration, ContextPtr)
{
    const Strings files = MetadataReadHelper::listFiles(configuration);
    return processMetadataFiles(files, "parquet");
}

#if USE_AWS_S3
template Strings HudiMetadataParser<StorageS3::Configuration, S3DataLakeMetadataReadHelper>::getFiles(
    const StorageS3::Configuration & configuration, ContextPtr);
#endif

}
