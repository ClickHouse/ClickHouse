#include "MergeTreeDataPartCompact.h"
#include <DataTypes/NestedUtils.h>
#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Compression/CompressedReadBufferFromFile.h>


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
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const MutableDataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_)
    : IMergeTreeDataPart(storage_, name_, info_, data_part_storage_, Type::Compact, parent_part_)
{
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartCompact::getReader(
    const NamesAndTypesList & columns_to_read,
    const StorageSnapshotPtr & storage_snapshot,
    const MarkRanges & mark_ranges,
    UncompressedCache * uncompressed_cache,
    MarkCache * mark_cache,
    const AlterConversionsPtr & alter_conversions,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback) const
{
    auto read_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(shared_from_this(), alter_conversions);
    auto * load_marks_threadpool = reader_settings.read_settings.load_marks_asynchronously ? &read_info->getContext()->getLoadMarksThreadpool() : nullptr;

    return std::make_unique<MergeTreeReaderCompact>(
        read_info, columns_to_read, storage_snapshot, uncompressed_cache,
        mark_cache, mark_ranges, reader_settings, load_marks_threadpool,
        avg_value_size_hints, profile_callback);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartCompact::getWriter(
    const NamesAndTypesList & columns_list,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & writer_settings,
    const MergeTreeIndexGranularity & computed_index_granularity)
{
    NamesAndTypesList ordered_columns_list;
    std::copy_if(columns_list.begin(), columns_list.end(), std::back_inserter(ordered_columns_list),
        [this](const auto & column) { return getColumnPosition(column.name) != std::nullopt; });

    /// Order of writing is important in compact format
    ordered_columns_list.sort([this](const auto & lhs, const auto & rhs)
        { return *getColumnPosition(lhs.name) < *getColumnPosition(rhs.name); });

    return std::make_unique<MergeTreeDataPartWriterCompact>(
        shared_from_this(), ordered_columns_list, metadata_snapshot,
        indices_to_recalc, getMarksFileExtension(),
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

    auto mrk_checksum = checksums.files.find(DATA_FILE_NAME + getMarksFileExtension());
    if (mrk_checksum != checksums.files.end())
        total_size.marks += mrk_checksum->second.file_size;
}

void MergeTreeDataPartCompact::loadIndexGranularityImpl(
    MergeTreeIndexGranularity & index_granularity_, const MergeTreeIndexGranularityInfo & index_granularity_info_,
    size_t columns_count, const IDataPartStorage & data_part_storage_)
{
    if (!index_granularity_info_.mark_type.adaptive)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MergeTreeDataPartCompact cannot be created with non-adaptive granulary.");

    auto marks_file_path = index_granularity_info_.getMarksFilePath("data");
    if (!data_part_storage_.exists(marks_file_path))
        throw Exception(
            ErrorCodes::NO_FILE_IN_DATA_PART,
            "Marks file '{}' doesn't exist",
            std::string(fs::path(data_part_storage_.getFullPath()) / marks_file_path));

    size_t marks_file_size = data_part_storage_.getFileSize(marks_file_path);

    std::unique_ptr<ReadBufferFromFileBase> buffer = data_part_storage_.readFile(
        marks_file_path, ReadSettings().adjustBufferSize(marks_file_size), marks_file_size, std::nullopt);

    std::unique_ptr<ReadBuffer> marks_reader;
    bool marks_compressed = index_granularity_info_.mark_type.compressed;
    if (marks_compressed)
        marks_reader = std::make_unique<CompressedReadBufferFromFile>(std::move(buffer));
    else
        marks_reader = std::move(buffer);

    while (!marks_reader->eof())
    {
        marks_reader->ignore(columns_count * sizeof(MarkInCompressedFile));
        size_t granularity;
        readBinaryLittleEndian(granularity, *marks_reader);
        index_granularity_.appendMark(granularity);
    }

    if (!marks_compressed && index_granularity_.getMarksCount() * index_granularity_info_.getMarkSizeInBytes(columns_count) != marks_file_size)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all marks from file {}", marks_file_path);

    index_granularity_.setInitialized();
}

void MergeTreeDataPartCompact::loadIndexGranularity()
{
    if (columns.empty())
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No columns in part {}", name);

    loadIndexGranularityImpl(index_granularity, index_granularity_info, columns.size(), getDataPartStorage());
}

bool MergeTreeDataPartCompact::hasColumnFiles(const NameAndTypePair & column) const
{
    if (!getColumnPosition(column.getNameInStorage()))
        return false;

    auto bin_checksum = checksums.files.find(DATA_FILE_NAME_WITH_EXTENSION);
    auto mrk_checksum = checksums.files.find(DATA_FILE_NAME + getMarksFileExtension());

    return (bin_checksum != checksums.files.end() && mrk_checksum != checksums.files.end());
}

std::optional<time_t> MergeTreeDataPartCompact::getColumnModificationTime(const String & /* column_name */) const
{
    return getDataPartStorage().getFileLastModified(DATA_FILE_NAME_WITH_EXTENSION).epochTime();
}

void MergeTreeDataPartCompact::checkConsistency(bool require_part_metadata) const
{
    checkConsistencyBase();
    String mrk_file_name = DATA_FILE_NAME + getMarksFileExtension();

    if (!checksums.empty())
    {
        /// count.txt should be present even in non custom-partitioned parts
        if (!checksums.files.contains("count.txt"))
            throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No checksum for count.txt");

        if (require_part_metadata)
        {
            if (!checksums.files.contains(mrk_file_name))
                throw Exception(
                    ErrorCodes::NO_FILE_IN_DATA_PART,
                    "No marks file checksum for column in part {}",
                    getDataPartStorage().getFullPath());
            if (!checksums.files.contains(DATA_FILE_NAME_WITH_EXTENSION))
                throw Exception(
                    ErrorCodes::NO_FILE_IN_DATA_PART,
                    "No data file checksum for in part {}",
                    getDataPartStorage().getFullPath());
        }
    }
    else
    {
        {
            /// count.txt should be present even in non custom-partitioned parts
            std::string file_path = "count.txt";
            if (!getDataPartStorage().exists(file_path) || getDataPartStorage().getFileSize(file_path) == 0)
                throw Exception(
                    ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                    "Part {} is broken: {} is empty",
                    getDataPartStorage().getRelativePath(),
                    std::string(fs::path(getDataPartStorage().getFullPath()) / file_path));
        }

        /// Check that marks are nonempty and have the consistent size with columns number.

        if (getDataPartStorage().exists(mrk_file_name))
        {
            UInt64 file_size = getDataPartStorage().getFileSize(mrk_file_name);
             if (!file_size)
                throw Exception(
                    ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                    "Part {} is broken: {} is empty.",
                    getDataPartStorage().getRelativePath(),
                    std::string(fs::path(getDataPartStorage().getFullPath()) / mrk_file_name));

            UInt64 expected_file_size = index_granularity_info.getMarkSizeInBytes(columns.size()) * index_granularity.getMarksCount();
            if (expected_file_size != file_size)
                throw Exception(
                    ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                    "Part {} is broken: bad size of marks file '{}': {}, must be: {}",
                    getDataPartStorage().getRelativePath(),
                    std::string(fs::path(getDataPartStorage().getFullPath()) / mrk_file_name),
                    file_size, expected_file_size);
        }
    }
}

bool MergeTreeDataPartCompact::isStoredOnRemoteDisk() const
{
    return getDataPartStorage().isStoredOnRemoteDisk();
}

bool MergeTreeDataPartCompact::isStoredOnRemoteDiskWithZeroCopySupport() const
{
    return getDataPartStorage().supportZeroCopyReplication();
}

MergeTreeDataPartCompact::~MergeTreeDataPartCompact()
{
    removeIfNeeded();
}

}
