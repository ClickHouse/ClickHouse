#include "MergeTreeDataPartWide.h"

#include <optional>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/HashingWriteBuffer.h>
#include <Core/Defines.h>
#include <Common/SipHash.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/localBackup.h>
#include <Compression/CompressionInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/DirectoryIterator.h>
#include <common/logger_useful.h>
#include <common/JSON.h>

#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>


namespace DB
{

// namespace
// {
// }

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int EXPECTED_END_OF_FILE;
    extern const int CORRUPTED_DATA;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int BAD_TTL_FILE;
    extern const int CANNOT_UNLINK;
}


// static ReadBufferFromFile openForReading(const String & path)
// {
//     return ReadBufferFromFile(path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
// }

MergeTreeDataPartWide::MergeTreeDataPartWide( 
       MergeTreeData & storage_,
        const String & name_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const DiskSpace::DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, index_granularity_info_, disk_, relative_path_)
{
}

MergeTreeDataPartWide::MergeTreeDataPartWide(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const DiskSpace::DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, info_, index_granularity_info_, disk_, relative_path_)
{
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartWide::getReader(
    const NamesAndTypesList & columns_to_read,
    const MarkRanges & mark_ranges,
    UncompressedCache * uncompressed_cache,
    MarkCache * mark_cache,
    const ReaderSettings & reader_settings,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback) const
{
    return std::make_unique<MergeTreeReaderWide>(shared_from_this(), columns_to_read, uncompressed_cache,
        mark_cache, mark_ranges, reader_settings, avg_value_size_hints, profile_callback);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartWide::getWriter(
    const NamesAndTypesList & columns_list,
    const CompressionCodecPtr & default_codec,
    const WriterSettings & writer_settings) const
{
    return std::make_unique<MergeTreeDataPartWriterWide>(
        getFullPath(), storage, columns_list,
        index_granularity_info.marks_file_extension,
        default_codec, writer_settings);
}        


/// Takes into account the fact that several columns can e.g. share their .size substreams.
/// When calculating totals these should be counted only once.
ColumnSize MergeTreeDataPartWide::getColumnSizeImpl(
    const String & column_name, const IDataType & type, std::unordered_set<String> * processed_substreams) const
{
    ColumnSize size;
    if (checksums.empty())
        return size;

    type.enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
    {
        String file_name = IDataType::getFileNameForStream(column_name, substream_path);

        if (processed_substreams && !processed_substreams->insert(file_name).second)
            return;

        auto bin_checksum = checksums.files.find(file_name + ".bin");
        if (bin_checksum != checksums.files.end())
        {
            size.data_compressed += bin_checksum->second.file_size;
            size.data_uncompressed += bin_checksum->second.uncompressed_size;
        }

        auto mrk_checksum = checksums.files.find(file_name + index_granularity_info.marks_file_extension);
        if (mrk_checksum != checksums.files.end())
            size.marks += mrk_checksum->second.file_size;
    }, {});

    return size;
}

/** Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
  * If no checksums are present returns the name of the first physically existing column.
  */
String MergeTreeDataPartWide::getColumnNameWithMinumumCompressedSize() const
{
    const auto & storage_columns = storage.getColumns().getAllPhysical();
    const std::string * minimum_size_column = nullptr;
    UInt64 minimum_size = std::numeric_limits<UInt64>::max();

    for (const auto & column : storage_columns)
    {
        if (!hasColumnFiles(column.name, *column.type))
            continue;

        const auto size = getColumnSizeImpl(column.name, *column.type, nullptr).data_compressed;
        if (size < minimum_size)
        {
            minimum_size = size;
            minimum_size_column = &column.name;
        }
    }

    if (!minimum_size_column)
        throw Exception("Could not find a column of minimum size in MergeTree, part " + getFullPath(), ErrorCodes::LOGICAL_ERROR);

    return *minimum_size_column;
}

UInt64 MergeTreeDataPartWide::calculateTotalSizeOnDisk(const String & from)
{
    Poco::File cur(from);
    if (cur.isFile())
        return cur.getSize();
    std::vector<std::string> files;
    cur.list(files);
    UInt64 res = 0;
    for (const auto & file : files)
        res += calculateTotalSizeOnDisk(from + file);
    return res;
}

void MergeTreeDataPartWide::loadIndexGranularity()
{
    String full_path = getFullPath();
    index_granularity_info.changeGranularityIfRequired(full_path);

    if (columns.empty())
        throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

    /// We can use any column, it doesn't matter
    std::string marks_file_path = index_granularity_info.getMarksFilePath(full_path + escapeForFileName(columns.front().name));
    if (!Poco::File(marks_file_path).exists())
        throw Exception("Marks file '" + marks_file_path + "' doesn't exist", ErrorCodes::NO_FILE_IN_DATA_PART);

    size_t marks_file_size = Poco::File(marks_file_path).getSize();

    if (!index_granularity_info.is_adaptive)
    {
        size_t marks_count = marks_file_size / index_granularity_info.mark_size_in_bytes;
        index_granularity.resizeWithFixedGranularity(marks_count, index_granularity_info.fixed_index_granularity); /// all the same
    }
    else
    {
        ReadBufferFromFile buffer(marks_file_path, marks_file_size, -1);
        while (!buffer.eof())
        {
            buffer.seek(sizeof(size_t) * 2, SEEK_CUR); /// skip offset_in_compressed file and offset_in_decompressed_block
            size_t granularity;
            readIntBinary(granularity, buffer);
            index_granularity.appendMark(granularity);
        }

        if (index_granularity.getMarksCount() * index_granularity_info.mark_size_in_bytes != marks_file_size)
            throw Exception("Cannot read all marks from file " + marks_file_path, ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    index_granularity.setInitialized();
}

// void MergeTreeDataPartWide::accumulateColumnSizes(ColumnToSize & column_to_size) const
// {
//     std::shared_lock<std::shared_mutex> part_lock(columns_lock);

//     for (const NameAndTypePair & name_type : storage.getColumns().getAllPhysical())
//     {
//         IDataType::SubstreamPath path;
//         name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
//         {
//             Poco::File bin_file(getFullPath() + IDataType::getFileNameForStream(name_type.name, substream_path) + ".bin");
//             if (bin_file.exists())
//                 column_to_size[name_type.name] += bin_file.getSize();
//         }, path);
//     }
// }



// bool MergeTreeDataPartWide::hasColumnFiles(const String & column_name, const IDataType & type) const
// {
//     bool res = true;

//     type.enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
//     {
//         String file_name = IDataType::getFileNameForStream(column_name, substream_path);

//         auto bin_checksum = checksums.files.find(file_name + ".bin");
//         auto mrk_checksum = checksums.files.find(file_name + index_granularity_info.marks_file_extension);

//         if (bin_checksum == checksums.files.end() || mrk_checksum == checksums.files.end())
//             res = false;
//     }, {});

//     return res;
// }

}
