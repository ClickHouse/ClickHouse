#include <algorithm>
#include <optional>

#include <Poco/File.h>
#include <Poco/DirectoryIterator.h>

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <DataStreams/MarkInCompressedFile.h>
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
}


IMergeTreeDataPart::Checksums checkDataPart(
    const String & full_path,
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

    String path = full_path;
    if (!path.empty() && path.back() != '/')
        path += "/";

    NamesAndTypesList columns_txt;

    {
        ReadBufferFromFile buf(path + "columns.txt");
        columns_txt.readText(buf);
        assertEOF(buf);
    }

    if (columns_txt != columns_list)
        throw Exception("Columns doesn't match in part " + path
            + ". Expected: " + columns_list.toString()
            + ". Found: " + columns_txt.toString(), ErrorCodes::CORRUPTED_DATA);

    /// Real checksums based on contents of data. Must correspond to checksums.txt. If not - it means the data is broken.
    IMergeTreeDataPart::Checksums checksums_data;

    auto checksum_compressed_file = [](const String & file_path)
    {
        ReadBufferFromFile file_buf(file_path);
        HashingReadBuffer compressed_hashing_buf(file_buf);
        CompressedReadBuffer uncompressing_buf(compressed_hashing_buf);
        HashingReadBuffer uncompressed_hashing_buf(uncompressing_buf);

        uncompressed_hashing_buf.tryIgnore(std::numeric_limits<size_t>::max());
        return IMergeTreeDataPart::Checksums::Checksum
        {
            compressed_hashing_buf.count(), compressed_hashing_buf.getHash(),
            uncompressed_hashing_buf.count(), uncompressed_hashing_buf.getHash()
        };
    };

    if (part_type == MergeTreeDataPartType::COMPACT)
    {
        const auto & file_name = MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION;
        checksums_data.files[file_name] = checksum_compressed_file(path + file_name);
    }
    else if (part_type == MergeTreeDataPartType::WIDE)
    {
        for (const auto & column : columns_list)
        {
            column.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
            {
                String file_name = IDataType::getFileNameForStream(column.name, substream_path) + ".bin";
                checksums_data.files[file_name] = checksum_compressed_file(path + file_name);
            }, {});
        }
    }
    else
    {
        throw Exception("Unknown type in part " + path, ErrorCodes::UNKNOWN_PART_TYPE);
    }

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(path); dir_it != dir_end; ++dir_it)
    {
        const String & file_name = dir_it.name();
        auto checksum_it = checksums_data.files.find(file_name);
        if (checksum_it == checksums_data.files.end() && file_name != "checksums.txt" && file_name != "columns.txt")
        {
            ReadBufferFromFile file_buf(dir_it->path());
            HashingReadBuffer hashing_buf(file_buf);
            hashing_buf.tryIgnore(std::numeric_limits<size_t>::max());
            checksums_data.files[file_name] = IMergeTreeDataPart::Checksums::Checksum(hashing_buf.count(), hashing_buf.getHash());
        }
    }

    /// Checksums from file checksums.txt. May be absent. If present, they are subsequently compared with the actual data checksums.
    IMergeTreeDataPart::Checksums checksums_txt;

    if (require_checksums || Poco::File(path + "checksums.txt").exists())
    {
        ReadBufferFromFile buf(path + "checksums.txt");
        checksums_txt.read(buf);
        assertEOF(buf);
    }

    if (is_cancelled())
        return {};

    if (require_checksums || !checksums_txt.files.empty())
        checksums_txt.checkEqual(checksums_data, true);

    return checksums_data;
}

IMergeTreeDataPart::Checksums checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    bool require_checksums,
    std::function<bool()> is_cancelled)
{
    return checkDataPart(
        data_part->getFullPath(),
        data_part->columns,
        data_part->getType(),
        require_checksums,
        is_cancelled);
}

}
