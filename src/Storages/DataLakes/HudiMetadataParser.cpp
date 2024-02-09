#include <Storages/DataLakes/HudiMetadataParser.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/logger_useful.h>
#include <base/find_symbols.h>
#include <Poco/String.h>
#include "config.h"
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/**
    * Useful links:
    * - https://hudi.apache.org/tech-specs/
    * - https://hudi.apache.org/docs/file_layouts/
    */

/**
    * Hudi tables store metadata files and data files.
    * Metadata files are stored in .hoodie/metadata directory. Though unlike DeltaLake and Iceberg,
    * metadata is not required in order to understand which files we need to read, moreover,
    * for Hudi metadata does not always exist.
    *
    * There can be two types of data files
    * 1. base files (columnar file formats like Apache Parquet/Orc)
    * 2. log files
    * Currently we support reading only `base files`.
    * Data file name format:
    * [File Id]_[File Write Token]_[Transaction timestamp].[File Extension]
    *
    * To find needed parts we need to find out latest part file for every file group for every partition.
    * Explanation why:
    *    Hudi reads in and overwrites the entire table/partition with each update.
    *    Hudi controls the number of file groups under a single partition according to the
    *    hoodie.parquet.max.file.size option. Once a single Parquet file is too large, Hudi creates a second file group.
    *    Each file group is identified by File Id.
    */
std::vector<String> listFiles(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const String & prefix, const String & suffix)
{
    auto key = std::filesystem::path(configuration.getPath()) / prefix;
    RelativePathsWithMetadata files_with_metadata;
    object_storage->listObjects(key, files_with_metadata, 0);
    Strings res;
    for (const auto & file_with_metadata : files_with_metadata)
    {
        const auto & filename = file_with_metadata->relative_path;
        if (filename.ends_with(suffix))
            res.push_back(filename);
    }
    LOG_TRACE(getLogger("DataLakeMetadataReadHelper"), "Listed {} files", res.size());
    return res;
}

Strings HudiMetadataParser::getFiles(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    ContextPtr)
{
    auto log = getLogger("HudiMetadataParser");

    const auto keys = listFiles(object_storage, *configuration, "", Poco::toLower(configuration->format));

    using Partition = std::string;
    using FileID = std::string;
    struct FileInfo
    {
        String key;
        UInt64 timestamp = 0;
    };
    std::unordered_map<Partition, std::unordered_map<FileID, FileInfo>> data_files;

    for (const auto & key : keys)
    {
        auto key_file = std::filesystem::path(key);
        Strings file_parts;
        const String stem = key_file.stem();
        splitInto<'_'>(file_parts, stem);
        if (file_parts.size() != 3)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected format for file: {}", key);

        const auto partition = key_file.parent_path().stem();
        const auto & file_id = file_parts[0];
        const auto timestamp = parse<UInt64>(file_parts[2]);

        auto & file_info = data_files[partition][file_id];
        if (file_info.timestamp == 0 || file_info.timestamp < timestamp)
        {
            file_info.key = key;
            file_info.timestamp = timestamp;
        }
    }

    Strings result;
    for (auto & [partition, partition_data] : data_files)
    {
        LOG_TRACE(log, "Adding {} data files from partition {}", partition, partition_data.size());
        for (auto & [file_id, file_data] : partition_data)
            result.push_back(std::move(file_data.key));
    }
    return result;
}

}
