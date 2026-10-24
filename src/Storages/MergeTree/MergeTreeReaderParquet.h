#pragma once

#include "config.h"

#if USE_PARQUET

#include <Storages/MergeTree/IMergeTreeReader.h>
#include <IO/ReadBufferFromFileBase.h>

namespace DB
{

namespace Parquet
{
struct Reader;
struct ColumnsCacheOptions;
}

/// Reads data parts in Parquet format. See `MergeTreeDataPartParquet`.
///
/// The granules of such parts are the row groups of the Parquet file, which are usually large,
/// so the reader can read a part of a row group and continue reading it on the next call.
class MergeTreeReaderParquet : public IMergeTreeReader
{
public:
    MergeTreeReaderParquet(
        MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
        NamesAndTypesList columns_,
        const VirtualFields & virtual_fields_,
        const StorageSnapshotPtr & storage_snapshot_,
        const MergeTreeSettingsPtr & storage_settings_,
        UncompressedCache * uncompressed_cache_,
        ColumnsCache * columns_cache_,
        MarkCache * mark_cache_,
        MarkRanges mark_ranges_,
        MergeTreeReaderSettings settings_,
        ValueSizeMap avg_value_size_hints_,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_);

    ~MergeTreeReaderParquet() override;

    size_t readRows(size_t from_mark, size_t current_range_last_mark,
                    bool continue_reading, size_t max_rows_to_read,
                    MutableColumns & res_columns, Columns & shared_columns) override;

    bool canReadIncompleteGranules() const override { return true; }

    void updateAllMarkRanges(const MarkRanges & ranges) override;

private:
    /// How to obtain the requested column (an element of `columns_to_read`) from the columns read from the file.
    struct RequestedColumn
    {
        /// Position of the column read from the file in `sample_block`. Unset if the column is missing in the part.
        std::optional<size_t> position_in_sample_block;
        /// If set, the requested column is a subcolumn of the column read from the file.
        std::optional<String> subcolumn_name;
        /// Only the array offsets of the column read from the file are needed (for a missing column of `Nested`).
        bool only_offsets = false;
    };

    /// A column of the part, which is read from the file.
    struct ColumnInFile
    {
        NameAndTypePair column_in_part;
        size_t position_in_part = 0;
        /// The values are stored in the ClickHouse binary format.
        bool binary = false;
    };

    /// The state of reading the file. It is created anew when the reader has to go back to a row group
    /// that was already read, because the decoder of a column chunk can only move forward.
    struct State;

    void fillColumns();
    void findColumnForOffsetsOfMissingNested(size_t pos, std::optional<size_t> & position_in_part);
    size_t addColumnInFile(const NameAndTypePair & column_in_part, size_t position_in_part);

    /// Null if the columns cache is not used.
    std::shared_ptr<const Parquet::ColumnsCacheOptions> getColumnsCacheOptions() const;

    void initState();
    void seek(size_t row_group);
    void finishRowGroup(size_t row_group);
    /// Decodes rows [start_row, start_row + num_rows) of the row group and appends them to `res_columns`.
    void readRowsFromRowGroup(size_t row_group, size_t start_row, size_t num_rows, MutableColumns & res_columns);
    /// Decodes rows [start_row, start_row + num_rows) of the row group, returns the columns as in the file.
    Columns decodeRows(size_t row_group, size_t start_row, size_t num_rows);
    /// Appends rows [start, start + num_rows) of the columns as in the file to `res_columns`.
    void appendRows(const Columns & columns_from_file, size_t start, size_t num_rows, MutableColumns & res_columns);

    ReadBufferFromFileBase::ProfileCallback profile_callback;

    std::vector<RequestedColumn> requested_columns;
    std::vector<ColumnInFile> columns_in_file;

    /// The columns read by the Parquet reader, with names as in the file and with the types of the part
    /// (or `String` for the columns stored in binary format).
    Block sample_block;

    std::unique_ptr<State> state;
};

}

#endif
