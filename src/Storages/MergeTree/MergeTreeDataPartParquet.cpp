#include <Storages/MergeTree/MergeTreeDataPartParquet.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/MergeTreeReaderParquet.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterParquet.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

#include "config.h"


namespace DB
{

namespace Setting
{
    extern const SettingsBool use_streaming_marks_compression;
}

namespace ErrorCodes
{
    extern const int NO_FILE_IN_DATA_PART;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int CORRUPTED_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

MergeTreeDataPartParquet::MergeTreeDataPartParquet(
    const MergeTreeData & storage_,
    const MergeTreeSettings & storage_settings,
    const String & name_,
    const MergeTreePartInfo & info_,
    const MutableDataPartStoragePtr & data_part_storage_,
    const IMergeTreeDataPart * parent_part_,
    PartDirIntent intent)
    : IMergeTreeDataPart(storage_, storage_settings, name_, info_, data_part_storage_, Type::Parquet, parent_part_, intent)
{
}

Strings MergeTreeDataPartParquet::getPreferredFileOrder() const
{
    Strings preferred_order = COMMON_METADATA_FILES;

    /// Files for partition key columns MinMax indices
    preferred_order.append_range(getMinMaxIndex()->getProbablyWrittenFiles(*this));

    /// Data marks file is used for loadIndexGranularity
    preferred_order.push_back(DATA_FILE_NAME + getMarksFileExtension());
    preferred_order.push_back("primary" + getIndexExtension(true));
    preferred_order.push_back("primary" + getIndexExtension(false));

    /// Files with statistics. Statistics are written as separate files
    /// in packed parts to avoid double bufferization in packed archive.
    for (const auto & [filename, _] : checksums.files)
    {
        if (filename.ends_with(STATS_FILE_SUFFIX))
            preferred_order.push_back(filename);
    }

    return preferred_order;
}

MergeTreeReaderPtr createMergeTreeReaderParquet(
    const MergeTreeDataPartInfoForReaderPtr & read_info,
    const NamesAndTypesList & columns_to_read,
    const StorageSnapshotPtr & storage_snapshot,
    const MergeTreeSettingsPtr & storage_settings,
    const MarkRanges & mark_ranges,
    const VirtualFields & virtual_fields,
    UncompressedCache * uncompressed_cache,
    ColumnsCache * columns_cache,
    MarkCache * mark_cache,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback);

MergeTreeReaderPtr createMergeTreeReaderParquet(
    const MergeTreeDataPartInfoForReaderPtr & read_info,
    const NamesAndTypesList & columns_to_read,
    const StorageSnapshotPtr & storage_snapshot,
    const MergeTreeSettingsPtr & storage_settings,
    const MarkRanges & mark_ranges,
    const VirtualFields & virtual_fields,
    UncompressedCache * uncompressed_cache,
    ColumnsCache * columns_cache,
    MarkCache * mark_cache,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback)
{
#if USE_PARQUET
    return std::make_unique<MergeTreeReaderParquet>(
        read_info, columns_to_read, virtual_fields,
        storage_snapshot, storage_settings, uncompressed_cache, columns_cache,
        mark_cache, mark_ranges, reader_settings,
        avg_value_size_hints, profile_callback);
#else
    UNUSED(read_info, columns_to_read, storage_snapshot, storage_settings, mark_ranges, virtual_fields, uncompressed_cache,
        columns_cache, mark_cache, reader_settings, avg_value_size_hints, profile_callback);
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Data parts in Parquet format are not supported: ClickHouse was built without Parquet");
#endif
}

MergeTreeDataPartWriterPtr createMergeTreeDataPartParquetWriter(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list,
    const ColumnPositions & column_positions,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & writer_settings,
    MergeTreeIndexGranularityPtr computed_index_granularity);

MergeTreeDataPartWriterPtr createMergeTreeDataPartParquetWriter(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list,
    const ColumnPositions & column_positions,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & writer_settings,
    MergeTreeIndexGranularityPtr computed_index_granularity)
{
#if USE_PARQUET
    NamesAndTypesList ordered_columns_list;
    std::copy_if(columns_list.begin(), columns_list.end(), std::back_inserter(ordered_columns_list),
        [&column_positions](const auto & column) { return column_positions.contains(column.name); });

    /// The columns of the Parquet file are in the same order as the columns of the part.
    ordered_columns_list.sort([&column_positions](const auto & lhs, const auto & rhs)
        { return column_positions.at(lhs.name) < column_positions.at(rhs.name); });

    return std::make_unique<MergeTreeDataPartWriterParquet>(
        data_part_name_, logger_name_, serializations_, data_part_storage_,
        index_granularity_info_, storage_settings_, ordered_columns_list, metadata_snapshot,
        indices_to_recalc, marks_file_extension_,
        default_codec_, writer_settings, std::move(computed_index_granularity));
#else
    UNUSED(data_part_name_, logger_name_, serializations_, data_part_storage_, index_granularity_info_, storage_settings_,
        columns_list, column_positions, metadata_snapshot, indices_to_recalc, marks_file_extension_, default_codec_,
        writer_settings, computed_index_granularity);
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Data parts in Parquet format are not supported: ClickHouse was built without Parquet");
#endif
}


void MergeTreeDataPartParquet::calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const
{
    String marks_file_name = DATA_FILE_NAME + getMarksFileExtension();

    auto mrk_checksum = checksums.files.find(marks_file_name);
    if (mrk_checksum != checksums.files.end())
        total_size.marks += mrk_checksum->second.file_size;

    auto data_checksum = checksums.files.find(DATA_FILE_NAME_WITH_EXTENSION);
    if (data_checksum == checksums.files.end())
        return;

    /// The whole file, including the metadata and the indices of the Parquet file.
    total_size.data_compressed += data_checksum->second.file_size;

    if (written_column_sizes)
    {
        for (const auto & [name, size] : *written_column_sizes)
        {
            total_size.data_uncompressed += size.data_uncompressed;
            each_columns_size[name] = size;
        }
        return;
    }

    /// The marks contain the sizes of the data of each column in each row group,
    /// and the final mark contains the uncompressed sizes of the columns (see `MergeTreeDataPartWriterParquet`).
    size_t marks_count = index_granularity ? index_granularity->getMarksCount() : 0;
    const auto & columns = getColumns();
    if (marks_count == 0 || columns.empty() || mrk_checksum == checksums.files.end() || !index_granularity->hasFinalMark())
    {
        total_size.data_uncompressed += data_checksum->second.file_size;
        return;
    }

    auto info_for_read = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(shared_from_this(), std::make_shared<AlterConversions>());
    MergeTreeMarksLoader marks_loader(
        info_for_read,
        /*mark_cache=*/ nullptr,
        marks_file_name,
        marks_count,
        index_granularity_info,
        /*save_marks_in_cache=*/ false,
        storage.getContext()->getReadSettings(),
        /*load_marks_threadpool=*/ nullptr,
        columns.size(),
        storage.getContext()->getSettingsRef()[Setting::use_streaming_marks_compression]);
    auto marks = marks_loader.loadMarks();

    std::vector<ColumnSize> sizes(columns.size());
    for (size_t mark = 0; mark < marks_count; ++mark)
    {
        bool is_final_mark = mark + 1 == marks_count;
        for (size_t column = 0; column < columns.size(); ++column)
        {
            const auto & range = marks->getMark(mark, column);
            if (is_final_mark)
                sizes[column].data_uncompressed += range.offset_in_decompressed_block;
            else
                sizes[column].data_compressed += range.offset_in_decompressed_block;
        }
    }

    size_t column = 0;
    for (const auto & name_and_type : columns)
    {
        total_size.data_uncompressed += sizes[column].data_uncompressed;
        each_columns_size[name_and_type.name] = sizes[column];
        ++column;
    }
}

UInt64 MergeTreeDataPartParquet::calculateBytesUncompressedOnDisk() const
{
    ColumnSizeByName each_columns_size;
    ColumnSize total_size;
    calculateEachColumnSizes(each_columns_size, total_size);
    return checksums.getTotalSizeUncompressedOnDisk() + total_size.data_uncompressed;
}

void MergeTreeDataPartParquet::loadIndexGranularity()
{
    if (getColumns().empty())
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No columns in part {}", name);

    /// The marks file has the same layout as in Compact parts.
    MergeTreeDataPartCompact::loadIndexGranularityImpl(
        index_granularity,
        index_granularity_info,
        getColumns().size(),
        getDataPartStorage(),
        *storage.getSettings());
}

void MergeTreeDataPartParquet::loadMarksToCache(const Names & column_names, MarkCache * mark_cache) const
{
    if (column_names.empty() || !mark_cache)
        return;

    auto context = storage.getContext();
    auto info_for_read = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(shared_from_this(), std::make_shared<AlterConversions>());

    LOG_TEST(getLogger("MergeTreeDataPartParquet"), "Loading marks into mark cache for columns {} of part {}", toString(column_names), name);

    MergeTreeMarksLoader loader(
        info_for_read,
        mark_cache,
        index_granularity_info.getMarksFilePath(DATA_FILE_NAME),
        index_granularity->getMarksCount(),
        index_granularity_info,
        /*save_marks_in_cache=*/ true,
        context->getReadSettings(),
        /*load_marks_threadpool_=*/ nullptr,
        getColumns().size(),
        context->getSettingsRef()[Setting::use_streaming_marks_compression]);

    loader.loadMarks();
}

void MergeTreeDataPartParquet::removeMarksFromCache(MarkCache * mark_cache) const
{
    if (!mark_cache)
        return;

    auto mark_path = index_granularity_info.getMarksFilePath(DATA_FILE_NAME);
    auto key = MarkCache::hash(getDataPartStorage().getDiskName() + ":" + (fs::path(getRelativePathOfActivePart()) / mark_path).string());
    mark_cache->remove(key);
}

bool MergeTreeDataPartParquet::hasColumnFiles(const NameAndTypePair & column) const
{
    if (!getColumnPosition(column.getNameInStorage()))
        return false;

    auto data_checksum = checksums.files.find(DATA_FILE_NAME_WITH_EXTENSION);
    auto mrk_checksum = checksums.files.find(DATA_FILE_NAME + getMarksFileExtension());

    return (data_checksum != checksums.files.end() && mrk_checksum != checksums.files.end());
}

std::optional<time_t> MergeTreeDataPartParquet::getColumnModificationTime(const String & /* column_name */) const
{
    return getDataPartStorage().getFileLastModified(DATA_FILE_NAME_WITH_EXTENSION).epochTime();
}

void MergeTreeDataPartParquet::doCheckConsistency(bool require_part_metadata) const
{
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

    /// Checksums regenerated from the files on disk by `loadChecksums` describe those files as they are,
    /// so they cannot vouch for a marks file torn by a crash: check the shape of the marks files directly
    /// in that case too, as when there are no checksums at all.
    if (!hasTrustworthyChecksums())
    {
        {
            /// count.txt should be present even in non custom-partitioned parts
            std::string file_path = "count.txt";
            if (!getDataPartStorage().existsFile(file_path) || getDataPartStorage().getFileSize(file_path) == 0)
                throw Exception(
                    ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                    "Part {} is broken: {} is empty",
                    getDataPartStorage().getRelativePath(),
                    std::string(fs::path(getDataPartStorage().getFullPath()) / file_path));
        }

        if (getDataPartStorage().existsFile(mrk_file_name) && !getDataPartStorage().existsFile(DATA_FILE_NAME_WITH_EXTENSION))
            throw Exception(
                ErrorCodes::NO_FILE_IN_DATA_PART,
                "Part {} is broken: no data file {} for marks file {}",
                getDataPartStorage().getRelativePath(),
                DATA_FILE_NAME_WITH_EXTENSION,
                mrk_file_name);

        /// Check that marks are nonempty and have the consistent size with columns number.
        if (getDataPartStorage().existsFile(mrk_file_name))
        {
            UInt64 file_size = getDataPartStorage().getFileSize(mrk_file_name);
            if (!file_size)
                throw Exception(
                    ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                    "Part {} is broken: {} is empty.",
                    getDataPartStorage().getRelativePath(),
                    std::string(fs::path(getDataPartStorage().getFullPath()) / mrk_file_name));

            /// The size of compressed marks does not follow from the number of marks, so check their
            /// decompressed size instead.
            UInt64 marks_payload_size = index_granularity_info.mark_type.compressed ? readFile(mrk_file_name)->ignoreAll() : file_size;
            size_t num_columns = getColumns().size();
            UInt64 expected_payload_size = index_granularity_info.getMarkSizeInBytes(num_columns) * index_granularity->getMarksCount();
            if (expected_payload_size != marks_payload_size)
                throw Exception(
                    ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                    "Part {} is broken: bad size of marks in file '{}': {}, must be: {}",
                    getDataPartStorage().getRelativePath(),
                    std::string(fs::path(getDataPartStorage().getFullPath()) / mrk_file_name),
                    marks_payload_size, expected_payload_size);

            /// Each mark points to a range of the data file, which must be inside the file.
            size_t marks_count = index_granularity->getMarksCount();
            if (marks_count != 0)
            {
                auto info_for_read = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(shared_from_this(), std::make_shared<AlterConversions>());
                MergeTreeMarksLoader marks_loader(
                    info_for_read,
                    /*mark_cache=*/ nullptr,
                    mrk_file_name,
                    marks_count,
                    index_granularity_info,
                    /*save_marks_in_cache=*/ false,
                    storage.getContext()->getReadSettings(),
                    /*load_marks_threadpool=*/ nullptr,
                    num_columns,
                    storage.getContext()->getSettingsRef()[Setting::use_streaming_marks_compression]);
                auto marks = marks_loader.loadMarks();

                UInt64 data_file_size = getDataPartStorage().getFileSize(DATA_FILE_NAME_WITH_EXTENSION);
                /// The final mark has the uncompressed sizes of the columns instead of the sizes of the data.
                size_t marks_with_data = index_granularity->hasFinalMark() ? marks_count - 1 : marks_count;
                for (size_t mark = 0; mark < marks_with_data; ++mark)
                {
                    for (size_t column = 0; column < num_columns; ++column)
                    {
                        const auto & range = marks->getMark(mark, column);
                        if (range.offset_in_compressed_file > data_file_size
                            || range.offset_in_decompressed_block > data_file_size - range.offset_in_compressed_file)
                            throw Exception(
                                ErrorCodes::CORRUPTED_DATA,
                                "Part {} is broken: data file {} has {} bytes, but its marks in {} point to range of {} bytes at offset {}",
                                getDataPartStorage().getFullPath(),
                                DATA_FILE_NAME_WITH_EXTENSION,
                                data_file_size,
                                mrk_file_name,
                                range.offset_in_decompressed_block,
                                range.offset_in_compressed_file);
                    }
                }
            }
        }
    }
}

bool MergeTreeDataPartParquet::isStoredOnRemoteDisk() const
{
    return getDataPartStorage().isStoredOnRemoteDisk();
}

bool MergeTreeDataPartParquet::isStoredOnReadonlyDisk() const
{
    return getDataPartStorage().isReadonly();
}

bool MergeTreeDataPartParquet::isStoredOnRemoteDiskWithZeroCopySupport() const
{
    return getDataPartStorage().supportZeroCopyReplication();
}

MergeTreeDataPartParquet::~MergeTreeDataPartParquet()
{
    try
    {
        removeIfNeeded();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
