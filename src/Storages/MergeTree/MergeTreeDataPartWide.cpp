#include "MergeTreeDataPartWide.h"
#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <DataTypes/NestedUtils.h>
#include <Common/quoteString.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_FILE_IN_DATA_PART;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int LOGICAL_ERROR;
}

namespace MergeTreeSetting
{
    extern MergeTreeSettingsBool enable_index_granularity_compression;
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

MergeTreeReaderPtr createMergeTreeReaderWide(
    const MergeTreeDataPartInfoForReaderPtr & read_info,
    const NamesAndTypesList & columns_to_read,
    const StorageSnapshotPtr & storage_snapshot,
    const MarkRanges & mark_ranges,
    const VirtualFields & virtual_fields,
    UncompressedCache * uncompressed_cache,
    MarkCache * mark_cache,
    DeserializationPrefixesCache * deserialization_prefixes_cache,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback)
{
    return std::make_unique<MergeTreeReaderWide>(
        read_info,
        columns_to_read,
        virtual_fields,
        storage_snapshot,
        uncompressed_cache,
        mark_cache,
        deserialization_prefixes_cache,
        mark_ranges,
        reader_settings,
        avg_value_size_hints,
        profile_callback,
        CLOCK_MONOTONIC_COARSE);
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
    MergeTreeIndexGranularityPtr computed_index_granularity)
{
    return std::make_unique<MergeTreeDataPartWriterWide>(
        data_part_name_, logger_name_, serializations_, data_part_storage_,
        index_granularity_info_, storage_settings_, columns_list,
        metadata_snapshot, virtual_columns, indices_to_recalc, stats_to_recalc_,
        marks_file_extension_,
        default_codec_, writer_settings, std::move(computed_index_granularity));
}


/// Takes into account the fact that several columns can e.g. share their .size substreams.
/// When calculating totals these should be counted only once.
ColumnSize MergeTreeDataPartWide::getColumnSizeImpl(
    const NameAndTypePair & column, std::unordered_set<String> * processed_substreams, std::optional<Block> columns_sample) const
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
    }, column.type, columns_sample && columns_sample->has(column.name) ? columns_sample->getByName(column.name).column : getColumnSample(column));

    return size;
}

void MergeTreeDataPartWide::loadIndexGranularityImpl(
    MergeTreeIndexGranularityPtr & index_granularity_ptr,
    MergeTreeIndexGranularityInfo & index_granularity_info_,
    const IDataPartStorage & data_part_storage_,
    const std::string & any_column_file_name,
    const MergeTreeSettings & storage_settings)
{
    index_granularity_info_.changeGranularityIfRequired(data_part_storage_);

    /// We can use any column, it doesn't matter
    std::string marks_file_path = index_granularity_info_.getMarksFilePath(any_column_file_name);
    if (!data_part_storage_.existsFile(marks_file_path))
        throw Exception(
            ErrorCodes::NO_FILE_IN_DATA_PART, "Marks file '{}' doesn't exist",
            std::string(fs::path(data_part_storage_.getFullPath()) / marks_file_path));

    size_t marks_file_size = data_part_storage_.getFileSize(marks_file_path);
    size_t fixed_granularity = index_granularity_info_.fixed_index_granularity;

    if (!index_granularity_info_.mark_type.adaptive && !index_granularity_info_.mark_type.compressed)
    {
        /// The most easy way - no need to read the file, everything is known from its size.
        size_t marks_count = marks_file_size / index_granularity_info_.getMarkSizeInBytes();
        index_granularity_ptr = std::make_shared<MergeTreeIndexGranularityConstant>(fixed_granularity, fixed_granularity, marks_count, false);
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
                index_granularity_ptr->appendMark(granularity);
            }
        }

        if (!index_granularity_info_.mark_type.adaptive)
        {
            index_granularity_ptr = std::make_shared<MergeTreeIndexGranularityConstant>(fixed_granularity, fixed_granularity, marks_count, false);
        }
        else if (storage_settings[MergeTreeSetting::enable_index_granularity_compression])
        {
            if (auto new_granularity_ptr = index_granularity_ptr->optimize())
                index_granularity_ptr = std::move(new_granularity_ptr);
        }
    }
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

    loadIndexGranularityImpl(index_granularity, index_granularity_info, getDataPartStorage(), *any_column_filename, *storage.getSettings());
}

void MergeTreeDataPartWide::loadMarksToCache(const Names & column_names, MarkCache * mark_cache) const
{
    if (column_names.empty() || !mark_cache)
        return;

    std::vector<std::unique_ptr<MergeTreeMarksLoader>> loaders;

    auto context = storage.getContext();
    auto read_settings = context->getReadSettings();
    auto * load_marks_threadpool = read_settings.load_marks_asynchronously ? &context->getLoadMarksThreadpool() : nullptr;
    auto info_for_read = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(shared_from_this(), std::make_shared<AlterConversions>());

    LOG_TEST(getLogger("MergeTreeDataPartWide"), "Loading marks into mark cache for columns {} of part {}", toString(column_names), name);

    for (const auto & column_name : column_names)
    {
        auto serialization = tryGetSerialization(column_name);
        if (!serialization)
            continue;

        serialization->enumerateStreams([&](const auto & subpath)
        {
            auto stream_name = getStreamNameForColumn(column_name, subpath, checksums);
            if (!stream_name)
                return;

            loaders.emplace_back(std::make_unique<MergeTreeMarksLoader>(
                info_for_read,
                mark_cache,
                index_granularity_info.getMarksFilePath(*stream_name),
                index_granularity->getMarksCount(),
                index_granularity_info,
                /*save_marks_in_cache=*/ true,
                read_settings,
                load_marks_threadpool,
                /*num_columns_in_mark=*/ 1));

            loaders.back()->startAsyncLoad();
        });
    }

    for (auto & loader : loaders)
        loader->loadMarks();
}

void MergeTreeDataPartWide::removeMarksFromCache(MarkCache * mark_cache) const
{
    if (!mark_cache)
        return;

    const auto & serializations = getSerializations();
    for (const auto & [column_name, serialization] : serializations)
    {
        serialization->enumerateStreams([&](const auto & subpath)
        {
            auto stream_name = getStreamNameForColumn(column_name, subpath, checksums);
            if (!stream_name)
                return;

            auto mark_path = index_granularity_info.getMarksFilePath(*stream_name);
            auto key = MarkCache::hash(fs::path(getRelativePathOfActivePart()) / mark_path);
            mark_cache->remove(key);
        });
    }
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
    try
    {
        removeIfNeeded();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
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
                    /// Skip ephemeral subcolumns that don't store any real data.
                    if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                        return;

                    auto stream_name = getStreamNameForColumn(name_type, substream_path, checksums);
                    if (!stream_name)
                        throw Exception(
                            ErrorCodes::NO_FILE_IN_DATA_PART,
                            "No stream ({}{}) file checksum for column {} in part {}",
                            ISerialization::getFileNameForStream(name_type, substream_path),
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

void MergeTreeDataPartWide::calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size, std::optional<Block> columns_sample) const
{
    std::unordered_set<String> processed_substreams;
    for (const auto & column : columns)
    {
        ColumnSize size = getColumnSizeImpl(column, &processed_substreams, columns_sample);
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
            if (rows_in_column > 0 && rows_in_column != rows_count)
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
