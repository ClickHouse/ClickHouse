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
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, disk_, relative_path_, Type::WIDE)
{
}

MergeTreeDataPartWide::MergeTreeDataPartWide(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, info_, disk_, relative_path_, Type::WIDE)
{
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartWide::getReader(
    const NamesAndTypesList & columns_to_read,
    const MarkRanges & mark_ranges,
    UncompressedCache * uncompressed_cache,
    MarkCache * mark_cache,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback) const
{
    return std::make_unique<MergeTreeReaderWide>(shared_from_this(), columns_to_read, uncompressed_cache,
        mark_cache, mark_ranges, reader_settings, avg_value_size_hints, profile_callback);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartWide::getWriter(
    const NamesAndTypesList & columns_list,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    const CompressionCodecPtr & default_codec,
    const MergeTreeWriterSettings & writer_settings,
    const MergeTreeIndexGranularity & computed_index_granularity) const
{
    return std::make_unique<MergeTreeDataPartWriterWide>(
        getFullPath(), storage, columns_list, indices_to_recalc,
        index_granularity_info.marks_file_extension,
        default_codec, writer_settings, computed_index_granularity);
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

ColumnSize MergeTreeDataPartWide::getTotalColumnsSize() const
{
    ColumnSize totals;
    std::unordered_set<String> processed_substreams;
    for (const NameAndTypePair & column : columns)
    {
        ColumnSize size = getColumnSizeImpl(column.name, *column.type, &processed_substreams);
        totals.add(size);
    }
    return totals;
}

ColumnSize MergeTreeDataPartWide::getColumnSize(const String & column_name, const IDataType & type) const
{
    return getColumnSizeImpl(column_name, type, nullptr);
}

void MergeTreeDataPartWide::loadIndexGranularity()
{
    String full_path = getFullPath();
    index_granularity_info.changeGranularityIfRequired(full_path);


    if (columns.empty())
        throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

    /// We can use any column, it doesn't matter
    std::string marks_file_path = index_granularity_info.getMarksFilePath(full_path + getFileNameForColumn(columns.front()));
    if (!Poco::File(marks_file_path).exists())
        throw Exception("Marks file '" + marks_file_path + "' doesn't exist", ErrorCodes::NO_FILE_IN_DATA_PART);

    size_t marks_file_size = Poco::File(marks_file_path).getSize();

    if (!index_granularity_info.is_adaptive)
    {
        size_t marks_count = marks_file_size / index_granularity_info.getMarkSizeInBytes();
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

        if (index_granularity.getMarksCount() * index_granularity_info.getMarkSizeInBytes() != marks_file_size)
            throw Exception("Cannot read all marks from file " + marks_file_path, ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    index_granularity.setInitialized();
}

MergeTreeDataPartWide::~MergeTreeDataPartWide()
{
    removeIfNeeded();
}

void MergeTreeDataPartWide::accumulateColumnSizes(ColumnToSize & column_to_size) const
{
    std::shared_lock<std::shared_mutex> part_lock(columns_lock);

    for (const NameAndTypePair & name_type : storage.getColumns().getAllPhysical())
    {
        IDataType::SubstreamPath path;
        name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
        {
            Poco::File bin_file(getFullPath() + IDataType::getFileNameForStream(name_type.name, substream_path) + ".bin");
            if (bin_file.exists())
                column_to_size[name_type.name] += bin_file.getSize();
        }, path);
    }
}

void MergeTreeDataPartWide::checkConsistency(bool require_part_metadata) const
{
    checkConsistencyBase();
    String path = getFullPath();

    if (!checksums.empty())
    {
        if (require_part_metadata)
        {
            for (const NameAndTypePair & name_type : columns)
            {
                IDataType::SubstreamPath stream_path;
                name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
                {
                    String file_name = IDataType::getFileNameForStream(name_type.name, substream_path);
                    String mrk_file_name = file_name + index_granularity_info.marks_file_extension;
                    String bin_file_name = file_name + ".bin";
                    if (!checksums.files.count(mrk_file_name))
                        throw Exception("No " + mrk_file_name + " file checksum for column " + name_type.name + " in part " + path,
                            ErrorCodes::NO_FILE_IN_DATA_PART);
                    if (!checksums.files.count(bin_file_name))
                        throw Exception("No " + bin_file_name + " file checksum for column " + name_type.name + " in part " + path,
                            ErrorCodes::NO_FILE_IN_DATA_PART);
                }, stream_path);
            }
        }
        
    }
    else
    {
        /// Check that all marks are nonempty and have the same size.
        std::optional<UInt64> marks_size;
        for (const NameAndTypePair & name_type : columns)
        {
            name_type.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
            {
                Poco::File file(IDataType::getFileNameForStream(name_type.name, substream_path) + index_granularity_info.marks_file_extension);

                /// Missing file is Ok for case when new column was added.
                if (file.exists())
                {
                    UInt64 file_size = file.getSize();

                    if (!file_size)
                        throw Exception("Part " + path + " is broken: " + file.path() + " is empty.",
                            ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);

                    if (!marks_size)
                        marks_size = file_size;
                    else if (file_size != *marks_size)
                        throw Exception("Part " + path + " is broken: marks have different sizes.",
                            ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
                }
            });
        }
    }
}

bool MergeTreeDataPartWide::hasColumnFiles(const String & column_name, const IDataType & type) const
{
    bool res = true;

    type.enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
    {
        String file_name = IDataType::getFileNameForStream(column_name, substream_path);

        auto bin_checksum = checksums.files.find(file_name + ".bin");
        auto mrk_checksum = checksums.files.find(file_name + index_granularity_info.marks_file_extension);

        if (bin_checksum == checksums.files.end() || mrk_checksum == checksums.files.end())
            res = false;
    }, {});

    return res;
}

NameToNameMap MergeTreeDataPartWide::createRenameMapForAlter(
    AlterAnalysisResult & analysis_result,
    const NamesAndTypesList & old_columns) const
{
    const auto & part_mrk_file_extension = index_granularity_info.marks_file_extension;
    NameToNameMap rename_map;

    for (const auto & index_name : analysis_result.removed_indices)
    {
        rename_map["skp_idx_" + index_name + ".idx"] = "";
        rename_map["skp_idx_" + index_name + part_mrk_file_extension] = "";
    }

    /// Collect counts for shared streams of different columns. As an example, Nested columns have shared stream with array sizes.
    std::map<String, size_t> stream_counts;
    for (const NameAndTypePair & column : old_columns)
    {
        column.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
        {
            ++stream_counts[IDataType::getFileNameForStream(column.name, substream_path)];
        }, {});
    }

    for (const auto & column : analysis_result.removed_columns)
    {
        if (hasColumnFiles(column.name, *column.type))
        {
            column.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
            {
                String file_name = IDataType::getFileNameForStream(column.name, substream_path);

                /// Delete files if they are no longer shared with another column.
                if (--stream_counts[file_name] == 0)
                {
                    rename_map[file_name + ".bin"] = "";
                    rename_map[file_name + part_mrk_file_extension] = "";
                }
            }, {});
        }
    }

    if (!analysis_result.conversions.empty())
    {
        /// Give proper names for temporary columns with conversion results.
        NamesWithAliases projection;
        projection.reserve(analysis_result.conversions.size());
        for (const auto & source_and_expression : analysis_result.conversions)
        {
            /// Column name for temporary filenames before renaming. NOTE The is unnecessarily tricky.
            const auto & source_name = source_and_expression.first;
            String temporary_column_name = source_name + " converting";

            projection.emplace_back(source_and_expression.second, temporary_column_name);

            /// After conversion, we need to rename temporary files into original.
            analysis_result.new_types.at(source_name)->enumerateStreams(
                [&](const IDataType::SubstreamPath & substream_path)
                {
                    /// Skip array sizes, because they cannot be modified in ALTER.
                    if (!substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes)
                        return;

                    String original_file_name = IDataType::getFileNameForStream(source_name, substream_path);
                    String temporary_file_name = IDataType::getFileNameForStream(temporary_column_name, substream_path);

                    rename_map[temporary_file_name + ".bin"] = original_file_name + ".bin";
                    rename_map[temporary_file_name + part_mrk_file_extension] = original_file_name + part_mrk_file_extension;
                }, {});
        }

        analysis_result.expression->add(ExpressionAction::project(projection));
    }

    return rename_map;
}

String MergeTreeDataPartWide::getFileNameForColumn(const NameAndTypePair & column) const
{
    String filename;
    column.type->enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
    {
        if (filename.empty())
            filename = IDataType::getFileNameForStream(column.name, substream_path);
    });
    return filename;
}

}
