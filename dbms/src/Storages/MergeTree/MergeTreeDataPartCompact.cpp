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


MergeTreeDataPartCompact::MergeTreeDataPartCompact(
       MergeTreeData & storage_,
        const String & name_,
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, disk_, relative_path_, Type::COMPACT)
{
}

MergeTreeDataPartCompact::MergeTreeDataPartCompact(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, info_, disk_, relative_path_, Type::COMPACT)
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

ColumnSize MergeTreeDataPartCompact::getTotalColumnsSize() const
{
    ColumnSize total_size;
    auto bin_checksum = checksums.files.find(DATA_FILE_NAME_WITH_EXTENSION);
    if (bin_checksum != checksums.files.end())
    {
        total_size.data_compressed += bin_checksum->second.file_size;
        total_size.data_compressed += bin_checksum->second.uncompressed_size;
    }

    auto mrk_checksum = checksums.files.find(DATA_FILE_NAME + index_granularity_info.marks_file_extension);
    if (mrk_checksum != checksums.files.end())
        total_size.marks += mrk_checksum->second.file_size;

    return total_size;
}

void MergeTreeDataPartCompact::loadIndexGranularity()
{
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
        /// Skip offsets for columns
        buffer.seek(columns.size() * sizeof(MarkInCompressedFile), SEEK_CUR);
        size_t granularity;
        readIntBinary(granularity, buffer);
        index_granularity.appendMark(granularity);
    }

    if (index_granularity.getMarksCount() * index_granularity_info.getMarkSizeInBytes(columns.size()) != marks_file_size)
        throw Exception("Cannot read all marks from file " + marks_file_path, ErrorCodes::CANNOT_READ_ALL_DATA);

    index_granularity.setInitialized();
}

bool MergeTreeDataPartCompact::hasColumnFiles(const String & column_name, const IDataType &) const
{
    if (!getColumnPosition(column_name))
        return false;

    auto bin_checksum = checksums.files.find(DATA_FILE_NAME_WITH_EXTENSION);
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
        rename_map[data_temp_name + DATA_FILE_EXTENSION] = DATA_FILE_NAME_WITH_EXTENSION;
        rename_map[data_temp_name + part_mrk_file_extension] = DATA_FILE_NAME + part_mrk_file_extension;
    }

    return rename_map;
}

void MergeTreeDataPartCompact::checkConsistency(bool require_part_metadata)
{
    checkConsistencyBase();
    String path = getFullPath();
    String mrk_file_name = DATA_FILE_NAME + index_granularity_info.marks_file_extension;

    if (!checksums.empty())
    {
        /// count.txt should be present even in non custom-partitioned parts
        if (!checksums.files.count("count.txt"))
            throw Exception("No checksum for count.txt", ErrorCodes::NO_FILE_IN_DATA_PART);
        
        if (require_part_metadata)
        {
            if (!checksums.files.count(mrk_file_name))
                throw Exception("No marks file checksum for column in part " + path, ErrorCodes::NO_FILE_IN_DATA_PART);
            if (!checksums.files.count(DATA_FILE_NAME_WITH_EXTENSION))
                throw Exception("No data file checksum for in part " + path, ErrorCodes::NO_FILE_IN_DATA_PART);
        }
    }
    else
    {
        {
            /// count.txt should be present even in non custom-partitioned parts
            Poco::File file(path + "count.txt");
            if (!file.exists() || file.getSize() == 0)
                throw Exception("Part " + path + " is broken: " + file.path() + " is empty", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
        }
        
        /// Check that marks are nonempty and have the consistent size with columns number.
        Poco::File file(path + mrk_file_name);

        if (file.exists())
        {
            UInt64 file_size = file.getSize();
             if (!file_size)
                throw Exception("Part " + path + " is broken: " + file.path() + " is empty.",
                    ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
            
            UInt64 expected_file_size = index_granularity_info.getMarkSizeInBytes(columns.size()) * index_granularity.getMarksCount();
            if (expected_file_size != file_size)
                throw Exception(
                    "Part " + path + " is broken: bad size of marks file '" + file.path() + "': " + std::to_string(file_size) + ", must be: " + std::to_string(expected_file_size),
                    ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
    
        }
    }
}

MergeTreeDataPartCompact::~MergeTreeDataPartCompact()
{
    removeIfNeeded();
}

}
