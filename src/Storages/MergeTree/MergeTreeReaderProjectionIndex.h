#pragma once

#include <Storages/MergeTree/IMergeTreeReader.h>

namespace DB
{

class MergeTreeReaderProjectionIndex : public IMergeTreeReader
{
public:
    MergeTreeReaderProjectionIndex(const IMergeTreeReader * main_reader_, ProjectionIndexBitmaps projection_index_bitmaps_);

    size_t readRows(
        size_t from_mark,
        size_t current_task_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        size_t offset,
        Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return main_reader->canReadIncompleteGranules(); }

private:
    /// Delegates to the main reader to determine if reading incomplete index granules is supported.
    const IMergeTreeReader * main_reader;

    /// Collection of bitmap indexes used to filter rows for the projection index.
    /// Each bitmap represents a condition, and rows must satisfy all to pass the filter.
    ProjectionIndexBitmaps projection_index_bitmaps;

    /// Current row position used when continuing reads across multiple calls.
    size_t current_row = 0;
};

}
