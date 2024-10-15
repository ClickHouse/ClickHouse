#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class MergeTreeDataPartCompact;
using DataPartCompactPtr = std::shared_ptr<const MergeTreeDataPartCompact>;

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

/// Base class of readers for compact parts.
class MergeTreeReaderCompact : public IMergeTreeReader
{
public:
    MergeTreeReaderCompact(
        MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
        NamesAndTypesList columns_,
        const VirtualFields & virtual_fields_,
        const StorageSnapshotPtr & storage_snapshot_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        MarkRanges mark_ranges_,
        MergeTreeReaderSettings settings_,
        ValueSizeMap avg_value_size_hints_,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
        clockid_t clock_type_);

    bool canReadIncompleteGranules() const final { return false; }

protected:
    void fillColumnPositions();
    NameAndTypePair getColumnConvertedToSubcolumnOfNested(const NameAndTypePair & column);
    void findPositionForMissedNested(size_t pos);

    using InputStreamGetter = ISerialization::InputStreamGetter;

    void readData(
        const NameAndTypePair & name_and_type,
        ColumnPtr & column,
        size_t rows_to_read,
        const InputStreamGetter & getter,
        ISerialization::SubstreamsCache & cache);

    void readPrefix(
        const NameAndTypePair & name_and_type,
        const InputStreamGetter & buffer_getter,
        const InputStreamGetter & buffer_getter_for_prefix,
        const ColumnNameLevel & name_level_for_offsets);

    void createColumnsForReading(Columns & res_columns) const;
    bool needSkipStream(size_t column_pos, const ISerialization::SubstreamPath & substream) const;

    const MergeTreeMarksLoaderPtr marks_loader;
    MergeTreeMarksGetterPtr marks_getter;

    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;

    /// Storage columns with collected separate arrays of Nested to columns of Nested type.
    /// They maybe be needed for finding offsets of missed Nested columns in parts.
    /// They are rarely used and are heavy to initialized, so we create them
    /// only on demand and cache in this field.
    std::optional<ColumnsDescription> storage_columns_with_collected_nested;

    /// Positions of columns in part structure.
    using ColumnPositions = std::vector<std::optional<size_t>>;
    ColumnPositions column_positions;

    /// Should we read full column or only it's offsets.
    /// Element of the vector is the level of the alternative stream.
    std::vector<ColumnNameLevel> columns_for_offsets;

    /// Mark to read in next 'readRows' call in case,
    /// when 'continue_reading' is true.
    size_t next_mark = 0;
};

}
