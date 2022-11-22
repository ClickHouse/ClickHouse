#include "MergeTreeDataPartWide.h"
#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <DataTypes/NestedUtils.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int LOGICAL_ERROR;
}


MergeTreeDataPartWide::MergeTreeDataPartWide(
       MergeTreeData & storage_,
        const String & name_,
        const DataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_)
    : IMergeTreeDataPart(storage_, name_, data_part_storage_, Type::Wide, parent_part_)
{
}

MergeTreeDataPartWide::MergeTreeDataPartWide(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_)
    : IMergeTreeDataPart(storage_, name_, info_, data_part_storage_, Type::Wide, parent_part_)
{
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartWide::getReader(
    const NamesAndTypesList & columns_to_read,
    const StorageMetadataPtr & metadata_snapshot,
    const MarkRanges & mark_ranges,
    UncompressedCache * uncompressed_cache,
    MarkCache * mark_cache,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback) const
{
    auto ptr = std::static_pointer_cast<const MergeTreeDataPartWide>(shared_from_this());
    return std::make_unique<MergeTreeReaderWide>(
        ptr, columns_to_read,
        metadata_snapshot, uncompressed_cache,
        mark_cache, mark_ranges, reader_settings,
        avg_value_size_hints, profile_callback);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartWide::getWriter(
    DataPartStorageBuilderPtr data_part_storage_builder,
    const NamesAndTypesList & columns_list,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & writer_settings,
    const MergeTreeIndexGranularity & computed_index_granularity) const
{
    return std::make_unique<MergeTreeDataPartWriterWide>(
        shared_from_this(), data_part_storage_builder,
        columns_list, metadata_snapshot, indices_to_recalc,
        index_granularity_info.marks_file_extension,
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
        String file_name = ISerialization::getFileNameForStream(column, substream_path);

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
    });

    return size;
}

void MergeTreeDataPartWide::loadIndexGranularity()
{
    index_granularity_info.changeGranularityIfRequired(data_part_storage);


    if (columns.empty())
        throw Exception("No columns in part " + name, ErrorCodes::NO_FILE_IN_DATA_PART);

    /// We can use any column, it doesn't matter
    std::string marks_file_path = index_granularity_info.getMarksFilePath(getFileNameForColumn(columns.front()));
    if (!data_part_storage->exists(marks_file_path))
        throw Exception(
            ErrorCodes::NO_FILE_IN_DATA_PART, "Marks file '{}' doesn't exist",
            std::string(fs::path(data_part_storage->getFullPath()) / marks_file_path));

    size_t marks_file_size = data_part_storage->getFileSize(marks_file_path);

    if (!index_granularity_info.is_adaptive)
    {
        size_t marks_count = marks_file_size / index_granularity_info.getMarkSizeInBytes();
        index_granularity.resizeWithFixedGranularity(marks_count, index_granularity_info.fixed_index_granularity); /// all the same
    }
    else
    {
        auto buffer = data_part_storage->readFile(marks_file_path, ReadSettings().adjustBufferSize(marks_file_size), marks_file_size, std::nullopt);
        while (!buffer->eof())
        {
            buffer->seek(sizeof(size_t) * 2, SEEK_CUR); /// skip offset_in_compressed file and offset_in_decompressed_block
            size_t granularity;
            readIntBinary(granularity, *buffer);
            index_granularity.appendMark(granularity);
        }

        if (index_granularity.getMarksCount() * index_granularity_info.getMarkSizeInBytes() != marks_file_size)
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all marks from file {}",
                std::string(fs::path(data_part_storage->getFullPath()) / marks_file_path));
    }

    index_granularity.setInitialized();
}

bool MergeTreeDataPartWide::isStoredOnRemoteDisk() const
{
    return data_part_storage->isStoredOnRemoteDisk();
}

bool MergeTreeDataPartWide::isStoredOnRemoteDiskWithZeroCopySupport() const
{
    return data_part_storage->supportZeroCopyReplication();
}

MergeTreeDataPartWide::~MergeTreeDataPartWide()
{
    removeIfNeeded();
}

void MergeTreeDataPartWide::checkConsistency(bool require_part_metadata) const
{
    checkConsistencyBase();
    //String path = getRelativePath();

    if (!checksums.empty())
    {
        if (require_part_metadata)
        {
            for (const auto & name_type : columns)
            {
                getSerialization(name_type.name)->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
                {
                    String file_name = ISerialization::getFileNameForStream(name_type, substream_path);
                    String mrk_file_name = file_name + index_granularity_info.marks_file_extension;
                    String bin_file_name = file_name + DATA_FILE_EXTENSION;

                    if (!checksums.files.contains(mrk_file_name))
                        throw Exception(
                            ErrorCodes::NO_FILE_IN_DATA_PART,
                            "No {} file checksum for column {} in part {} ",
                            mrk_file_name, name_type.name, data_part_storage->getFullPath());

                    if (!checksums.files.contains(bin_file_name))
                        throw Exception(
                            ErrorCodes::NO_FILE_IN_DATA_PART,
                            "No {} file checksum for column {} in part ",
                            bin_file_name, name_type.name, data_part_storage->getFullPath());
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
                auto file_path = ISerialization::getFileNameForStream(name_type, substream_path) + index_granularity_info.marks_file_extension;

                /// Missing file is Ok for case when new column was added.
                if (data_part_storage->exists(file_path))
                {
                    UInt64 file_size = data_part_storage->getFileSize(file_path);

                    if (!file_size)
                        throw Exception(
                            ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                            "Part {} is broken: {} is empty.",
                            data_part_storage->getFullPath(),
                            std::string(fs::path(data_part_storage->getFullPath()) / file_path));

                    if (!marks_size)
                        marks_size = file_size;
                    else if (file_size != *marks_size)
                        throw Exception(
                            ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                            "Part {} is broken: marks have different sizes.", data_part_storage->getFullPath());
                }
            });
        }
    }
}

bool MergeTreeDataPartWide::hasColumnFiles(const NameAndTypePair & column) const
{
    auto check_stream_exists = [this](const String & stream_name)
    {
        auto bin_checksum = checksums.files.find(stream_name + DATA_FILE_EXTENSION);
        auto mrk_checksum = checksums.files.find(stream_name + index_granularity_info.marks_file_extension);

        return bin_checksum != checksums.files.end() && mrk_checksum != checksums.files.end();
    };

    bool res = true;
    getSerialization(column.name)->enumerateStreams([&](const auto & substream_path)
    {
        String file_name = ISerialization::getFileNameForStream(column, substream_path);
        if (!check_stream_exists(file_name))
            res = false;
    });

    return res;
}

String MergeTreeDataPartWide::getFileNameForColumn(const NameAndTypePair & column) const
{
    String filename;
    getSerialization(column.name)->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
    {
        if (filename.empty())
            filename = ISerialization::getFileNameForStream(column, substream_path);
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
                    "and size of single value, but data part {} has {} rows", backQuote(column.name), rows_in_column, name, rows_count);
            }
        }
#endif
    }
}

}
