#pragma once

#include <Core/NamesAndTypes.h>
#include <Common/HashTable/HashMap.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>

namespace DB
{

/// Reads the data between pairs of marks in the same part. When reading consecutive ranges, avoids unnecessary seeks.
/// When ranges are almost consecutive, seeks are fast because they are performed inside the buffer.
/// Avoids loading the marks file if it is not needed (e.g. when reading the whole part).
class IMergeTreeReader : private boost::noncopyable
{
public:
    using ValueSizeMap = std::map<std::string, double>;
    using VirtualFields = std::unordered_map<String, Field>;
    using DeserializeBinaryBulkStateMap = std::map<std::string, ISerialization::DeserializeBinaryBulkStatePtr>;

    IMergeTreeReader(
        MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
        const NamesAndTypesList & columns_,
        const VirtualFields & virtual_fields_,
        const StorageSnapshotPtr & storage_snapshot_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        const MarkRanges & all_mark_ranges_,
        const MergeTreeReaderSettings & settings_,
        const ValueSizeMap & avg_value_size_hints_ = ValueSizeMap{});

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark.
    /// current_task_last mark is needed for asynchronous reading (mainly from remote fs).
    virtual size_t readRows(size_t from_mark, size_t current_task_last_mark,
                            bool continue_reading, size_t max_rows_to_read, Columns & res_columns) = 0;

    virtual bool canReadIncompleteGranules() const = 0;

    virtual ~IMergeTreeReader() = default;

    const ValueSizeMap & getAvgValueSizeHints() const;

    /// Add virtual columns that are not present in the block.
    void fillVirtualColumns(Columns & columns, size_t rows) const;

    /// Add columns from ordered_names that are not present in the block.
    /// Missing columns are added in the order specified by ordered_names.
    /// num_rows is needed in case if all res_columns are nullptr.
    void fillMissingColumns(Columns & res_columns, bool & should_evaluate_missing_defaults, size_t num_rows) const;
    /// Evaluate defaulted columns if necessary.
    void evaluateMissingDefaults(Block additional_columns, Columns & res_columns) const;

    /// If part metadata is not equal to storage metadata,
    /// then try to perform conversions of columns.
    void performRequiredConversions(Columns & res_columns) const;

    const NamesAndTypesList & getColumns() const { return requested_columns; }
    size_t numColumnsInResult() const { return requested_columns.size(); }

    size_t getFirstMarkToRead() const { return all_mark_ranges.front().begin; }

    MergeTreeDataPartInfoForReaderPtr data_part_info_for_read;

    virtual void prefetchBeginOfRange(Priority) {}

protected:
    /// Returns actual column name in part, which can differ from table metadata.
    String getColumnNameInPart(const NameAndTypePair & required_column) const;
    /// Returns actual column name and type in part, which can differ from table metadata.
    NameAndTypePair getColumnInPart(const NameAndTypePair & required_column) const;
    /// Returns actual serialization in part, which can differ from table metadata.
    SerializationPtr getSerializationInPart(const NameAndTypePair & required_column) const;
    /// Returns true if requested column is a subcolumn with offsets of Array which is part of Nested column.
    bool isSubcolumnOffsetsOfNested(const String & name_in_storage, const String & subcolumn_name) const;

    void checkNumberOfColumns(size_t num_columns_to_read) const;
    String getMessageForDiagnosticOfBrokenPart(size_t from_mark, size_t max_rows_to_read) const;

    /// avg_value_size_hints are used to reduce the number of reallocations when creating columns of variable size.
    ValueSizeMap avg_value_size_hints;
    /// Stores states for IDataType::deserializeBinaryBulk
    DeserializeBinaryBulkStateMap deserialize_binary_bulk_state_map;

    /// Actual column names and types of columns in part,
    /// which may differ from table metadata.
    NamesAndTypes columns_to_read;
    /// Actual serialization of columns in part.
    Serializations serializations;

    UncompressedCache * const uncompressed_cache;
    MarkCache * const mark_cache;

    MergeTreeReaderSettings settings;

    const StorageSnapshotPtr storage_snapshot;
    const MarkRanges all_mark_ranges;

    /// Position and level (of nesting).
    using ColumnNameLevel = std::optional<std::pair<String, size_t>>;

    /// In case of part of the nested column does not exist, offsets should be
    /// read, but only the offsets for the current column, that is why it
    /// returns pair of size_t, not just one.
    ColumnNameLevel findColumnForOffsets(const NameAndTypePair & column) const;

    NameSet partially_read_columns;

    /// Alter conversions, which must be applied on fly if required
    AlterConversionsPtr alter_conversions;

private:
    /// Columns that are requested to read.
    NamesAndTypesList original_requested_columns;

    /// The same as above but with converted Arrays to subcolumns of Nested.
    NamesAndTypesList requested_columns;

    /// Actual columns description in part.
    const ColumnsDescription & part_columns;

    /// Fields of virtual columns that were filled in previous stages.
    VirtualFields virtual_fields;
};

}
