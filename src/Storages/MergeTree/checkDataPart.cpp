#include <algorithm>
#include <optional>

#include <Poco/File.h>
#include <Poco/DirectoryIterator.h>

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/HashingReadBuffer.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric ReplicatedChecks;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_PART_TYPE;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MUNMAP;
    extern const int CANNOT_MREMAP;
}


bool isNotEnoughMemoryErrorCode(int code)
{
    /// Don't count the part as broken if there is not enough memory to load it.
    /// In fact, there can be many similar situations.
    /// But it is OK, because there is a safety guard against deleting too many parts.
    return code == ErrorCodes::MEMORY_LIMIT_EXCEEDED
        || code == ErrorCodes::CANNOT_ALLOCATE_MEMORY
        || code == ErrorCodes::CANNOT_MUNMAP
        || code == ErrorCodes::CANNOT_MREMAP;
}


IMergeTreeDataPart::Checksums checkDataPart(
    const DiskPtr & disk,
    const String & full_relative_path,
    const NamesAndTypesList & columns_list,
    const MergeTreeDataPartType & part_type,
    bool require_checksums,
    std::function<bool()> is_cancelled)
{
    /** Responsibility:
      * - read list of columns from columns.txt;
      * - read checksums if exist;
      * - validate list of columns and checksums
      */

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedChecks};

    String path = full_relative_path;
    if (!path.empty() && path.back() != '/')
        path += "/";

    NamesAndTypesList columns_txt;

    {
        auto buf = disk->readFile(path + "columns.txt");
        columns_txt.readText(*buf);
        assertEOF(*buf);
    }

    if (columns_txt != columns_list)
        throw Exception("Columns doesn't match in part " + path
            + ". Expected: " + columns_list.toString()
            + ". Found: " + columns_txt.toString(), ErrorCodes::CORRUPTED_DATA);

    /// Real checksums based on contents of data. Must correspond to checksums.txt. If not - it means the data is broken.
    IMergeTreeDataPart::Checksums checksums_data;

    /// This function calculates checksum for both compressed and decompressed contents of compressed file.
    auto checksum_compressed_file = [](const DiskPtr & disk_, const String & file_path)
    {
        auto file_buf = disk_->readFile(file_path);
        HashingReadBuffer compressed_hashing_buf(*file_buf);
        CompressedReadBuffer uncompressing_buf(compressed_hashing_buf);
        HashingReadBuffer uncompressed_hashing_buf(uncompressing_buf);

        uncompressed_hashing_buf.tryIgnore(std::numeric_limits<size_t>::max());
        return IMergeTreeDataPart::Checksums::Checksum
        {
            compressed_hashing_buf.count(), compressed_hashing_buf.getHash(),
            uncompressed_hashing_buf.count(), uncompressed_hashing_buf.getHash()
        };
    };

    /// First calculate checksums for columns data
    if (part_type == MergeTreeDataPartType::COMPACT)
    {
        const auto & file_name = MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION;
        checksums_data.files[file_name] = checksum_compressed_file(disk, path + file_name);
    }
    else if (part_type == MergeTreeDataPartType::WIDE)
    {
        for (const auto & column : columns_list)
        {
            column.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
            {
                String file_name = IDataType::getFileNameForStream(column.name, substream_path) + ".bin";
                checksums_data.files[file_name] = checksum_compressed_file(disk, path + file_name);
            }, {});
        }
    }
    else
    {
        throw Exception("Unknown type in part " + path, ErrorCodes::UNKNOWN_PART_TYPE);
    }

    /// Checksums from the rest files listed in checksums.txt. May be absent. If present, they are subsequently compared with the actual data checksums.
    IMergeTreeDataPart::Checksums checksums_txt;

    if (require_checksums || disk->exists(path + "checksums.txt"))
    {
        auto buf = disk->readFile(path + "checksums.txt");
        checksums_txt.read(*buf);
        assertEOF(*buf);
    }

    const auto & checksum_files_txt = checksums_txt.files;
    for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
    {
        const String & file_name = it->name();
        auto checksum_it = checksums_data.files.find(file_name);

        /// Skip files that we already calculated. Also skip metadata files that are not checksummed.
        if (checksum_it == checksums_data.files.end() && file_name != "checksums.txt" && file_name != "columns.txt")
        {
            auto txt_checksum_it = checksum_files_txt.find(file_name);
            if (txt_checksum_it == checksum_files_txt.end() || txt_checksum_it->second.uncompressed_size == 0)
            {
                /// The file is not compressed.
                auto file_buf = disk->readFile(it->path());
                HashingReadBuffer hashing_buf(*file_buf);
                hashing_buf.tryIgnore(std::numeric_limits<size_t>::max());
                checksums_data.files[file_name] = IMergeTreeDataPart::Checksums::Checksum(hashing_buf.count(), hashing_buf.getHash());
            }
            else /// If we have both compressed and uncompressed in txt, than calculate them
            {
                checksums_data.files[file_name] = checksum_compressed_file(disk, it->path());
            }
        }
    }

    if (is_cancelled())
        return {};

    if (require_checksums || !checksums_txt.files.empty())
        checksums_txt.checkEqual(checksums_data, true);

    return checksums_data;
}

IMergeTreeDataPart::Checksums checkDataPartInMemory(const DataPartInMemoryPtr & data_part)
{
    IMergeTreeDataPart::Checksums data_checksums;
    data_checksums.files["data.bin"] = data_part->calculateBlockChecksum();
    data_part->checksums.checkEqual(data_checksums, true);
    return data_checksums;
}

IMergeTreeDataPart::Checksums checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    bool require_checksums,
    std::function<bool()> is_cancelled)
{
    if (auto part_in_memory = asInMemoryPart(data_part))
        return checkDataPartInMemory(part_in_memory);

    return checkDataPart(
        data_part->volume->getDisk(),
        data_part->getFullRelativePath(),
        data_part->getColumns(),
        data_part->getType(),
        require_checksums,
        is_cancelled);
}

}
