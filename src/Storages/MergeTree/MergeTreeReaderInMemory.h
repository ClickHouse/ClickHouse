#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>


namespace DB
{

class MergeTreeDataPartInMemory;
using DataPartInMemoryPtr = std::shared_ptr<const MergeTreeDataPartInMemory>;

/// Reader for InMemory parts
class MergeTreeReaderInMemory : public IMergeTreeReader
{
public:
    MergeTreeReaderInMemory(
        DataPartInMemoryPtr data_part_,
        NamesAndTypesList columns_,
        const StorageMetadataPtr & metadata_snapshot_,
        MarkRanges mark_ranges_,
        MergeTreeReaderSettings settings_);

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(size_t from_mark, size_t current_tasl_last_mark,
                    bool continue_reading, size_t max_rows_to_read, Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return true; }

private:
    size_t total_rows_read = 0;
    DataPartInMemoryPtr part_in_memory;

    std::unordered_map<String, size_t> positions_for_offsets;
};

}
