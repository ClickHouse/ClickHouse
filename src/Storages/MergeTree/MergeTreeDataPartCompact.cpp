#include "MergeTreeDataPartCompact.h"
#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
}


MergeTreeDataPartCompact::MergeTreeDataPartCompact(
       MergeTreeData & storage_,
        const String & name_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, volume_, relative_path_, Type::COMPACT)
{
}

MergeTreeDataPartCompact::MergeTreeDataPartCompact(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, info_, volume_, relative_path_, Type::COMPACT)
{
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartCompact::getReader(
    const NamesAndTypesList & columns_to_read,
    const StorageMetadataPtr & metadata_snapshot,
    const MarkRanges & mark_ranges,
    UncompressedCache * uncompressed_cache,
    MarkCache * mark_cache,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback) const
{
    auto ptr = std::static_pointer_cast<const MergeTreeDataPartCompact>(shared_from_this());
    return std::make_unique<MergeTreeReaderCompact>(
        ptr, columns_to_read, metadata_snapshot, uncompressed_cache,
        mark_cache, mark_ranges, reader_settings,
        avg_value_size_hints, profile_callback);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartCompact::getWriter(
    const NamesAndTypesList & columns_list,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    const CompressionCodecPtr & default_codec_,
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
        shared_from_this(), ordered_columns_list, metadata_snapshot,
        indices_to_recalc, index_granularity_info.marks_file_extension,
        default_codec_, writer_settings, computed_index_granularity);
}


void MergeTreeDataPartCompact::calculateEachColumnSizes(ColumnSizeByName & /*each_columns_size*/, ColumnSize & total_size) const
{
    auto bin_checksum = checksums.files.find(DATA_FILE_NAME_WITH_EXTENSION);
    if (bin_checksum != checksums.files.end())
    {
        total_size.data_compressed += bin_checksum->second.file_size;
        total_size.data_uncompressed += bin_checksum->second.uncompressed_size;
    }

    auto mrk_checksum = checksums.files.find(DATA_FILE_NAME + index_granularity_info.marks_file_extension);
    if (mrk_checksum != checksums.files.end())
        total_size.marks += mrk_checksum->second.file_size;
}

void MergeTreeDataPartCompact::loadIndexGranularity()
{
    String full_path = getFullRelativePath();

    if (columns.empty())
        throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

    if (!index_granularity_info.is_adaptive)
        throw Exception("MergeTreeDataPartCompact cannot be created with non-adaptive granulary.", ErrorCodes::NOT_IMPLEMENTED);

    auto marks_file_path = index_granularity_info.getMarksFilePath(full_path + "data");
    if (!volume->getDisk()->exists(marks_file_path))
        throw Exception("Marks file '" + fullPath(volume->getDisk(), marks_file_path) + "' doesn't exist", ErrorCodes::NO_FILE_IN_DATA_PART);

    size_t marks_file_size = volume->getDisk()->getFileSize(marks_file_path);

    auto buffer = volume->getDisk()->readFile(marks_file_path, marks_file_size);
    while (!buffer->eof())
    {
        /// Skip offsets for columns
        buffer->seek(columns.size() * sizeof(MarkInCompressedFile), SEEK_CUR);
        size_t granularity;
        readIntBinary(granularity, *buffer);
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

void MergeTreeDataPartCompact::checkConsistency(bool require_part_metadata) const
{
    checkConsistencyBase();
    String path = getFullRelativePath();
    String mrk_file_name = DATA_FILE_NAME + index_granularity_info.marks_file_extension;

    if (!checksums.empty())
    {
        /// count.txt should be present even in non custom-partitioned parts
        if (!checksums.files.count("count.txt"))
            throw Exception("No checksum for count.txt", ErrorCodes::NO_FILE_IN_DATA_PART);

        if (require_part_metadata)
        {
            if (!checksums.files.count(mrk_file_name))
                throw Exception("No marks file checksum for column in part " + fullPath(volume->getDisk(), path), ErrorCodes::NO_FILE_IN_DATA_PART);
            if (!checksums.files.count(DATA_FILE_NAME_WITH_EXTENSION))
                throw Exception("No data file checksum for in part " + fullPath(volume->getDisk(), path), ErrorCodes::NO_FILE_IN_DATA_PART);
        }
    }
    else
    {
        {
            /// count.txt should be present even in non custom-partitioned parts
            auto file_path = path + "count.txt";
            if (!volume->getDisk()->exists(file_path) || volume->getDisk()->getFileSize(file_path) == 0)
                throw Exception("Part " + path + " is broken: " + fullPath(volume->getDisk(), file_path) + " is empty", ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
        }

        /// Check that marks are nonempty and have the consistent size with columns number.
        auto mrk_file_path = path + mrk_file_name;

        if (volume->getDisk()->exists(mrk_file_name))
        {
            UInt64 file_size = volume->getDisk()->getFileSize(mrk_file_name);
             if (!file_size)
                throw Exception("Part " + path + " is broken: " + fullPath(volume->getDisk(), mrk_file_name) + " is empty.",
                    ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);

            UInt64 expected_file_size = index_granularity_info.getMarkSizeInBytes(columns.size()) * index_granularity.getMarksCount();
            if (expected_file_size != file_size)
                throw Exception(
                    "Part " + path + " is broken: bad size of marks file '" + fullPath(volume->getDisk(), mrk_file_name) + "': " + std::to_string(file_size) + ", must be: " + std::to_string(expected_file_size),
                    ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);
        }
    }
}

MergeTreeDataPartCompact::~MergeTreeDataPartCompact()
{
    removeIfNeeded();
}

}
