#include "MergeTreeDataPartWide.h"
#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <DataTypes/NestedUtils.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_FILE_IN_DATA_PART;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int LOGICAL_ERROR;
}

MergeTreeDataPartWide::MergeTreeDataPartWide(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const MutableDataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_)
    : IMergeTreeDataPart(storage_, name_, info_, data_part_storage_, Type::Wide, parent_part_)
{
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartWide::getReader(
    const NamesAndTypesList & columns_to_read,
    const StorageSnapshotPtr & storage_snapshot,
    const MarkRanges & mark_ranges,
    const VirtualFields & virtual_fields,
    UncompressedCache * uncompressed_cache,
    MarkCache * mark_cache,
    const AlterConversionsPtr & alter_conversions,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback) const
{
    auto read_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(shared_from_this(), alter_conversions);
    return std::make_unique<MergeTreeReaderWide>(
        read_info,
        columns_to_read,
        virtual_fields,
        storage_snapshot,
        uncompressed_cache,
        mark_cache,
        mark_ranges,
        reader_settings,
        avg_value_size_hints,
        profile_callback);
}

MergeTreeDataPartWriterPtr createMergeTreeDataPartWideWriter(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list,
    const StorageMetadataPtr & metadata_snapshot,
    const VirtualsDescriptionPtr & virtual_columns,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    const ColumnsStatistics & stats_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & writer_settings,
    const MergeTreeIndexGranularity & computed_index_granularity)
{
    return std::make_unique<MergeTreeDataPartWriterWide>(
        data_part_name_, logger_name_, serializations_, data_part_storage_,
        index_granularity_info_, storage_settings_, columns_list,
        metadata_snapshot, virtual_columns, indices_to_recalc, stats_to_recalc_,
        marks_file_extension_,
        default_codec_, writer_settings, computed_index_granularity);
}


/// Takes into account the fact that several columns can e.g. share their .size substreams.
/// When calculating totals these should be counted only once.
ColumnSize MergeTreeDataPartWide::getColumnSizeImpl(
    const NameAndTypePair & column, std::unordered_set<String> * processed_substreams) const
{
    ColumnSize size;
    if (checksums.empty())
        return size;

    getSerialization(column.name)->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column, substream_path, checksums);

        if (!stream_name)
            return;

        if (processed_substreams && !processed_substreams->insert(*stream_name).second)
            return;

        auto bin_checksum = checksums.files.find(*stream_name + ".bin");
        if (bin_checksum != checksums.files.end())
        {
            size.data_compressed += bin_checksum->second.file_size;
            size.data_uncompressed += bin_checksum->second.uncompressed_size;
        }

        auto mrk_checksum = checksums.files.find(*stream_name + getMarksFileExtension());
        if (mrk_checksum != checksums.files.end())
            size.marks += mrk_checksum->second.file_size;
    });

    return size;
}

void MergeTreeDataPartWide::loadIndexGranularityImpl(
    MergeTreeIndexGranularity & index_granularity_, MergeTreeIndexGranularityInfo & index_granularity_info_,
    const IDataPartStorage & data_part_storage_, const std::string & any_column_file_name)
{
    index_granularity_info_.changeGranularityIfRequired(data_part_storage_);

    /// We can use any column, it doesn't matter
    std::string marks_file_path = index_granularity_info_.getMarksFilePath(any_column_file_name);
    if (!data_part_storage_.existsFile(marks_file_path))
        throw Exception(
            ErrorCodes::NO_FILE_IN_DATA_PART, "Marks file '{}' doesn't exist",
            std::string(fs::path(data_part_storage_.getFullPath()) / marks_file_path));

    size_t marks_file_size = data_part_storage_.getFileSize(marks_file_path);

    if (!index_granularity_info_.mark_type.adaptive && !index_granularity_info_.mark_type.compressed)
    {
        /// The most easy way - no need to read the file, everything is known from its size.
        size_t marks_count = marks_file_size / index_granularity_info_.getMarkSizeInBytes();
        index_granularity_.resizeWithFixedGranularity(marks_count, index_granularity_info_.fixed_index_granularity); /// all the same
    }
    else
    {
        auto marks_file = data_part_storage_.readFile(marks_file_path, getReadSettings().adjustBufferSize(marks_file_size), marks_file_size, std::nullopt);

        std::unique_ptr<ReadBuffer> marks_reader;
        if (!index_granularity_info_.mark_type.compressed)
            marks_reader = std::move(marks_file);
        else
            marks_reader = std::make_unique<CompressedReadBufferFromFile>(std::move(marks_file));

        size_t marks_count = 0;
        while (!marks_reader->eof())
        {
            MarkInCompressedFile mark;
            size_t granularity;

            readBinaryLittleEndian(mark.offset_in_compressed_file, *marks_reader);
            readBinaryLittleEndian(mark.offset_in_decompressed_block, *marks_reader);
            ++marks_count;

            if (index_granularity_info_.mark_type.adaptive)
            {
                readBinaryLittleEndian(granularity, *marks_reader);
                index_granularity_.appendMark(granularity);
            }
        }

        if (!index_granularity_info_.mark_type.adaptive)
            index_granularity_.resizeWithFixedGranularity(marks_count, index_granularity_info_.fixed_index_granularity); /// all the same
    }

    index_granularity_.setInitialized();
}

void MergeTreeDataPartWide::loadIndexGranularity()
{
    if (columns.empty())
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No columns in part {}", name);

    auto any_column_filename = getFileNameForColumn(columns.front());
    if (!any_column_filename)
        throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART,
            "There are no files for column {} in part {}",
            columns.front().name, getDataPartStorage().getFullPath());

    loadIndexGranularityImpl(index_granularity, index_granularity_info, getDataPartStorage(), *any_column_filename);
}


bool MergeTreeDataPartWide::isStoredOnRemoteDisk() const
{
    return getDataPartStorage().isStoredOnRemoteDisk();
}

bool MergeTreeDataPartWide::isStoredOnReadonlyDisk() const
{
    return getDataPartStorage().isReadonly();
}

bool MergeTreeDataPartWide::isStoredOnRemoteDiskWithZeroCopySupport() const
{
    return getDataPartStorage().supportZeroCopyReplication();
}

MergeTreeDataPartWide::~MergeTreeDataPartWide()
{
    removeIfNeeded();
}

void MergeTreeDataPartWide::doCheckConsistency(bool require_part_metadata) const
{
    std::string marks_file_extension = index_granularity_info.mark_type.getFileExtension();

    if (!checksums.empty())
    {
        if (require_part_metadata)
        {
            for (const auto & name_type : columns)
            {
                getSerialization(name_type.name)->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
                {
                    auto stream_name = getStreamNameForColumn(name_type, substream_path, checksums);
                    if (!stream_name)
                        throw Exception(
                            ErrorCodes::NO_FILE_IN_DATA_PART,
                            "No stream ({}) file checksum for column {} in part {}",
                            DATA_FILE_EXTENSION,
                            name_type.name,
                            getDataPartStorage().getFullPath());

                    auto mrk_file_name = *stream_name + marks_file_extension;
                    if (!checksums.files.contains(mrk_file_name))
                        throw Exception(
                            ErrorCodes::NO_FILE_IN_DATA_PART,
                            "No {} file checksum for column {} in part {} ",
                            mrk_file_name, name_type.name, getDataPartStorage().getFullPath());
                });
            }
        }
    }
    else
    {
        /// Check that all marks are nonempty and have the same size.
        std::optional<UInt64> marks_size;
        for (const auto & name_type : columns)
        {
            getSerialization(name_type.name)->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
            {
                auto stream_name = getStreamNameForColumn(name_type, substream_path, marks_file_extension, getDataPartStorage());

                /// Missing file is Ok for case when new column was added.
                if (!stream_name)
                    return;

                auto file_path = *stream_name + marks_file_extension;
                UInt64 file_size = getDataPartStorage().getFileSize(file_path);

                if (!file_size)
                    throw Exception(
                        ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                        "Part {} is broken: {} is empty.",
                        getDataPartStorage().getFullPath(),
                        std::string(fs::path(getDataPartStorage().getFullPath()) / file_path));

                if (!marks_size)
                    marks_size = file_size;
                else if (file_size != *marks_size)
                    throw Exception(
                        ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                        "Part {} is broken: marks have different sizes.", getDataPartStorage().getFullPath());
            });
        }
    }
}

bool MergeTreeDataPartWide::hasColumnFiles(const NameAndTypePair & column) const
{
    auto serialization = tryGetSerialization(column.name);
    if (!serialization)
        return false;
    auto marks_file_extension = index_granularity_info.mark_type.getFileExtension();

    bool res = true;
    serialization->enumerateStreams([&](const auto & substream_path)
    {
        auto stream_name = getStreamNameForColumn(column, substream_path, checksums);
        if (!stream_name || !checksums.files.contains(*stream_name + marks_file_extension))
            res = false;
    });

    return res;
}

std::optional<time_t> MergeTreeDataPartWide::getColumnModificationTime(const String & column_name) const
{
    try
    {
        auto stream_name = getStreamNameOrHash(column_name, checksums);
        if (!stream_name)
            return {};

        return getDataPartStorage().getFileLastModified(*stream_name + DATA_FILE_EXTENSION).epochTime();
    }
    catch (const fs::filesystem_error &)
    {
        return {};
    }
}

std::optional<String> MergeTreeDataPartWide::getFileNameForColumn(const NameAndTypePair & column) const
{
    std::optional<String> filename;

    /// Fallback for the case when serializations was not loaded yet (called from loadColumns())
    if (getSerializations().empty())
        return getStreamNameForColumn(column, {}, DATA_FILE_EXTENSION, getDataPartStorage());

    getSerialization(column.name)->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
    {
        if (!filename.has_value())
        {
            /// This method may be called when checksums are not initialized yet.
            if (!checksums.empty())
                filename = getStreamNameForColumn(column, substream_path, checksums);
            else
                filename = getStreamNameForColumn(column, substream_path, DATA_FILE_EXTENSION, getDataPartStorage());
        }
    });

    return filename;
}

void MergeTreeDataPartWide::calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const
{
    std::unordered_set<String> processed_substreams;
    for (const auto & column : columns)
    {
        ColumnSize size = getColumnSizeImpl(column, &processed_substreams);
        each_columns_size[column.name] = size;
        total_size.add(size);

#ifndef NDEBUG
        /// Most trivial types
        if (rows_count != 0
            && column.type->isValueRepresentedByNumber()
            && !column.type->haveSubtypes()
            && getSerialization(column.name)->getKind() == ISerialization::Kind::DEFAULT)
        {
            size_t rows_in_column = size.data_uncompressed / column.type->getSizeOfValueInMemory();
            if (rows_in_column != rows_count)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Column {} has rows count {} according to size in memory "
                    "and size of single value, but data part {} has {} rows",
                    backQuote(column.name), rows_in_column, name, rows_count);
            }
        }
#endif
    }
}

}
