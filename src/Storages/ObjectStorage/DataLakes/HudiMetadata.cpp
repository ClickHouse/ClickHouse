#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/ReadHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Storages/ObjectStorage/DataLakes/HudiMetadata.h>
#include <base/find_symbols.h>
#include <Poco/String.h>
#include <Common/logger_useful.h>

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
Strings HudiMetadata::getDataFilesImpl() const
{
    auto configuration_ptr = configuration.lock();
    auto log = getLogger("HudiMetadata");
    const auto keys = listFiles(*object_storage, *configuration_ptr, "", Poco::toLower(configuration_ptr->format));

    using Partition = std::string;
    using FileID = std::string;
    struct FileInfo
    {
        String key;
        UInt64 timestamp = 0;
    };
    std::unordered_map<Partition, std::unordered_map<FileID, FileInfo>> files;

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

        auto & file_info = files[partition][file_id];
        if (file_info.timestamp == 0 || file_info.timestamp < timestamp)
        {
            file_info.key = key;
            file_info.timestamp = timestamp;
        }
    }

    Strings result;
    for (auto & [partition, partition_data] : files)
    {
        LOG_TRACE(log, "Adding {} data files from partition {}", partition, partition_data.size());
        for (auto & [file_id, file_data] : partition_data)
            result.push_back(std::move(file_data.key));
    }
    return result;
}

HudiMetadata::HudiMetadata(ObjectStoragePtr object_storage_, ConfigurationObserverPtr configuration_, ContextPtr context_)
    : WithContext(context_), object_storage(object_storage_), configuration(configuration_)
{
}

Strings HudiMetadata::getDataFiles() const
{
    if (data_files.empty())
        data_files = getDataFilesImpl();
    return data_files;
}

}
