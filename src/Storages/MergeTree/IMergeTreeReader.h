#pragma once

#include <Core/NamesAndTypes.h>
#include <Common/HashTable/HashMap.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>

namespace DB
{

class IDataType;

/// Reads the data between pairs of marks in the same part. When reading consecutive ranges, avoids unnecessary seeks.
/// When ranges are almost consecutive, seeks are fast because they are performed inside the buffer.
/// Avoids loading the marks file if it is not needed (e.g. when reading the whole part).
class IMergeTreeReader : private boost::noncopyable
{
public:
    using ValueSizeMap = std::map<std::string, double>;
    using DeserializeBinaryBulkStateMap = std::map<std::string, ISerialization::DeserializeBinaryBulkStatePtr>;

    IMergeTreeReader(
        const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_,
        const StorageMetadataPtr & metadata_snapshot_,
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

    virtual ~IMergeTreeReader();

    const ValueSizeMap & getAvgValueSizeHints() const;

    /// Add columns from ordered_names that are not present in the block.
    /// Missing columns are added in the order specified by ordered_names.
    /// num_rows is needed in case if all res_columns are nullptr.
    void fillMissingColumns(Columns & res_columns, bool & should_evaluate_missing_defaults, size_t num_rows) const;
    /// Evaluate defaulted columns if necessary.
    void evaluateMissingDefaults(Block additional_columns, Columns & res_columns) const;

    /// If part metadata is not equal to storage metadata, than
    /// try to perform conversions of columns.
    void performRequiredConversions(Columns & res_columns) const;

    const NamesAndTypesList & getColumns() const { return columns; }
    size_t numColumnsInResult() const { return columns.size(); }

    size_t getFirstMarkToRead() const
    {
        return all_mark_ranges.front().begin;
    }

    MergeTreeData::DataPartPtr data_part;

protected:
    /// Returns actual column name in part, which can differ from table metadata.
    String getColumnNameInPart(const NameAndTypePair & required_column) const;
    /// Returns actual column name and type in part, which can differ from table metadata.
    NameAndTypePair getColumnInPart(const NameAndTypePair & required_column) const;

    void checkNumberOfColumns(size_t num_columns_to_read) const;

    /// avg_value_size_hints are used to reduce the number of reallocations when creating columns of variable size.
    ValueSizeMap avg_value_size_hints;
    /// Stores states for IDataType::deserializeBinaryBulk
    DeserializeBinaryBulkStateMap deserialize_binary_bulk_state_map;

    /// Columns that are read.
    NamesAndTypesList columns;

    UncompressedCache * uncompressed_cache;
    MarkCache * mark_cache;

    MergeTreeReaderSettings settings;

    const MergeTreeData & storage;
    StorageMetadataPtr metadata_snapshot;
    MarkRanges all_mark_ranges;

    using ColumnPosition = std::optional<size_t>;
    ColumnPosition findColumnForOffsets(const String & column_name) const;

private:
    /// Alter conversions, which must be applied on fly if required
    MergeTreeData::AlterConversions alter_conversions;
};

}
