#include "MergeTreeDataPartCompact.h"

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

#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>
#include <Storages/MergeTree/IMergeTreeReader.h>


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

MergeTreeDataPartCompact::MergeTreeDataPartCompact(
       MergeTreeData & storage_,
        const String & name_,
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, disk_, relative_path_)
{
}

MergeTreeDataPartCompact::MergeTreeDataPartCompact(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, info_, disk_, relative_path_)
{
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartCompact::getReader(
    const NamesAndTypesList & columns_to_read,
    const MarkRanges & mark_ranges,
    UncompressedCache * uncompressed_cache,
    MarkCache * mark_cache,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & /* profile_callback */) const
{
    /// FIXME maybe avoid shared_from_this
    return std::make_unique<MergeTreeReaderCompact>(
        shared_from_this(), columns_to_read, uncompressed_cache,
        mark_cache, mark_ranges, reader_settings, avg_value_size_hints);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartCompact::getWriter(
    const NamesAndTypesList & columns_list,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    const CompressionCodecPtr & default_codec,
    const MergeTreeWriterSettings & writer_settings,
    const MergeTreeIndexGranularity & computed_index_granularity) const
{
    NamesAndTypesList ordered_columns_list;
    std::copy_if(columns_list.begin(), columns_list.end(), std::back_inserter(ordered_columns_list),
        [this](const auto & column) { return getColumnPosition(column.name) != std::nullopt; });

    /// Order of writing is important in compact format
    ordered_columns_list.sort([this](const auto & lhs, const auto & rhs)
        { return *getColumnPosition(lhs.name) < *getColumnPosition(rhs.name); });

    return std::make_unique<MergeTreeDataPartWriterCompact>(
        getFullPath(), storage, ordered_columns_list, indices_to_recalc,
        index_granularity_info.marks_file_extension,
        default_codec, writer_settings, computed_index_granularity);
}

ColumnSize MergeTreeDataPartCompact::getColumnSize(const String & column_name, const IDataType & /* type */) const
{
    auto column_size = columns_sizes.find(column_name);
    if (column_size == columns_sizes.end())
        return {};
    return column_size->second;
}

ColumnSize MergeTreeDataPartCompact::getTotalColumnsSize() const
{
    ColumnSize totals;
    size_t marks_size = 0;
    for (const auto & column : columns)
    {
        auto column_size = getColumnSize(column.name, *column.type);
        totals.add(column_size);
        if (!marks_size && column_size.marks)
            marks_size = column_size.marks;
    }
    /// Marks are shared between all columns
    totals.marks = marks_size;
    return totals;
}

/** Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
  * If no checksums are present returns the name of the first physically existing column.
  */
String MergeTreeDataPartCompact::getColumnNameWithMinumumCompressedSize() const
{
    const auto & storage_columns = storage.getColumns().getAllPhysical();
    const std::string * minimum_size_column = nullptr;
    UInt64 minimum_size = std::numeric_limits<UInt64>::max();
    for (const auto & column : storage_columns)
    {
        if (!getColumnPosition(column.name))
            continue;

        auto size = getColumnSize(column.name, *column.type).data_compressed;
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

void MergeTreeDataPartCompact::loadIndexGranularity()
{
    index_granularity_info = MergeTreeIndexGranularityInfo{storage, getType(), columns.size()};
    String full_path = getFullPath();

    if (columns.empty())
        throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

    if (!index_granularity_info.is_adaptive)
        throw Exception("MergeTreeDataPartCompact cannot be created with non-adaptive granulary. TODO: help message", ErrorCodes::NOT_IMPLEMENTED);

    std::string marks_file_path = index_granularity_info.getMarksFilePath(full_path + "data");
    if (!Poco::File(marks_file_path).exists())
        throw Exception("Marks file '" + marks_file_path + "' doesn't exist", ErrorCodes::NO_FILE_IN_DATA_PART);

    size_t marks_file_size = Poco::File(marks_file_path).getSize();

    ReadBufferFromFile buffer(marks_file_path, marks_file_size);
    while (!buffer.eof())
    {
        size_t granularity;
        readIntBinary(granularity, buffer);
        index_granularity.appendMark(granularity);
        /// Skip offsets for columns
        buffer.seek(columns.size() * sizeof(MarkInCompressedFile), SEEK_CUR);
    }

    if (index_granularity.getMarksCount() * index_granularity_info.mark_size_in_bytes != marks_file_size)
        throw Exception("Cannot read all marks from file " + marks_file_path, ErrorCodes::CANNOT_READ_ALL_DATA);

    index_granularity.setInitialized();
}

bool MergeTreeDataPartCompact::hasColumnFiles(const String & column_name, const IDataType &) const
{
    if (!getColumnPosition(column_name))
        return false;

    auto bin_checksum = checksums.files.find(String(DATA_FILE_NAME) + DATA_FILE_EXTENSION);
    auto mrk_checksum = checksums.files.find(DATA_FILE_NAME + index_granularity_info.marks_file_extension);

    return (bin_checksum != checksums.files.end() && mrk_checksum != checksums.files.end());
}

NameToNameMap MergeTreeDataPartCompact::createRenameMapForAlter(
    AlterAnalysisResult & analysis_result,
    const NamesAndTypesList & /* old_columns */) const
{
    const auto & part_mrk_file_extension = index_granularity_info.marks_file_extension;
    NameToNameMap rename_map;

    for (const auto & index_name : analysis_result.removed_indices)
    {
        rename_map["skp_idx_" + index_name + ".idx"] = "";
        rename_map["skp_idx_" + index_name + part_mrk_file_extension] = "";
    }

    /// We have to rewrite all data if any column has been changed.
    if (!analysis_result.removed_columns.empty() || !analysis_result.conversions.empty())
    {
        if (!analysis_result.expression)
            analysis_result.expression = std::make_shared<ExpressionActions>(NamesAndTypesList(), storage.global_context);

        NameSet altered_columns;
        NamesWithAliases projection;

        for (const auto & column : analysis_result.removed_columns)
            altered_columns.insert(column.name);

        for (const auto & [source_name, result_name] : analysis_result.conversions)
        {
            altered_columns.insert(source_name);
            projection.emplace_back(result_name, source_name);
        }

        /// Add other part columns to read
        for (const auto & column : columns)
        {
            if (!altered_columns.count(column.name))
            {
                analysis_result.expression->addInput(column);
                projection.emplace_back(column.name, "");
            }
        }

        analysis_result.expression->add(ExpressionAction::project(projection));

        String data_temp_name = String(DATA_FILE_NAME) + "_converting";
        rename_map[data_temp_name + DATA_FILE_EXTENSION] = String(DATA_FILE_NAME) + DATA_FILE_EXTENSION;
        rename_map[data_temp_name + part_mrk_file_extension] = DATA_FILE_NAME + part_mrk_file_extension;
    }

    return rename_map;
}

void MergeTreeDataPartCompact::checkConsistency(bool require_part_metadata)
{
    UNUSED(require_part_metadata);
    /// FIXME implement for compact parts


    // String path = getFullPath();

    // if (!checksums.empty())
    // {
    //     if (!storage.primary_key_columns.empty() && !checksums.files.count("primary.idx"))
    //         throw Exception("No checksum for primary.idx", ErrorCodes::NO_FILE_IN_DATA_PART);

    //     if (require_part_metadata)
    //     {
    //         for (const NameAndTypePair & name_type : columns)
    //         {
    //             IDataType::SubstreamPath stream_path;
    //             name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
    //             {
    //                 String file_name = IDataType::getFileNameForStream(name_type.name, substream_path);
    //                 String mrk_file_name = file_name + index_granularity_info.marks_file_extension;
    //                 String bin_file_name = file_name + ".bin";
    //                 if (!checksums.files.count(mrk_file_name))
    //                     throw Exception("No " + mrk_file_name + " file checksum for column " + name_type.name + " in part " + path,
    //                         ErrorCodes::NO_FILE_IN_DATA_PART);
    //                 if (!checksums.files.count(bin_file_name))
    //                     throw Exception("No " + bin_file_name + " file checksum for column " + name_type.name + " in part " + path,
    //                         ErrorCodes::NO_FILE_IN_DATA_PART);
    //             }, stream_path);
    //         }
    //     }

    //     if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    //     {
    //         if (!checksums.files.count("count.txt"))
    //             throw Exception("No checksum for count.txt", ErrorCodes::NO_FILE_IN_DATA_PART);

    //         if (storage.partition_key_expr && !checksums.files.count("partition.dat"))
    //             throw Exception("No checksum for partition.dat", ErrorCodes::NO_FILE_IN_DATA_PART);

    //         if (!isEmpty())
    //         {
    //             for (const String & col_name : storage.minmax_idx_columns)
    //             {
    //                 if (!checksums.files.count("minmax_" + escapeForFileName(col_name) + ".idx"))
    //                     throw Exception("No minmax idx file checksum for column " + col_name, ErrorCodes::NO_FILE_IN_DATA_PART);
    //             }
    //         }
    //     }

    //     checksums.checkSizes(path);
    // }
    // else
    // {
    //     auto check_file_not_empty = [&path](const String & file_path)
    //     {
    //         Poco::File file(file_path);
    //         if (!file.exists() || file.getSize() == 0)
    //             throw Exception("Part " + path + " is broken: " + file_path + " is empty", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
    //         return file.getSize();
    //     };

    //     /// Check that the primary key index is not empty.
    //     if (!storage.primary_key_columns.empty())
    //         check_file_not_empty(path + "primary.idx");

    //     if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    //     {
    //         check_file_not_empty(path + "count.txt");

    //         if (storage.partition_key_expr)
    //             check_file_not_empty(path + "partition.dat");

    //         for (const String & col_name : storage.minmax_idx_columns)
    //             check_file_not_empty(path + "minmax_" + escapeForFileName(col_name) + ".idx");
    //     }

    //     /// Check that all marks are nonempty and have the same size.

    //     std::optional<UInt64> marks_size;
    //     for (const NameAndTypePair & name_type : columns)
    //     {
    //         name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
    //         {
    //             Poco::File file(IDataType::getFileNameForStream(name_type.name, substream_path) + index_granularity_info.marks_file_extension);

    //             /// Missing file is Ok for case when new column was added.
    //             if (file.exists())
    //             {
    //                 UInt64 file_size = file.getSize();

    //                 if (!file_size)
    //                     throw Exception("Part " + path + " is broken: " + file.path() + " is empty.",
    //                         ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);

    //                 if (!marks_size)
    //                     marks_size = file_size;
    //                 else if (file_size != *marks_size)
    //                     throw Exception("Part " + path + " is broken: marks have different sizes.",
    //                         ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
    //             }
    //         });
    //     }
    // }
}

MergeTreeDataPartCompact::~MergeTreeDataPartCompact()
{
    removeIfNeeded();
}

}
