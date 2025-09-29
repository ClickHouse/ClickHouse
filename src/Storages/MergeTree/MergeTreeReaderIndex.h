#pragma once

#include <Storages/MergeTree/IMergeTreeReader.h>

namespace DB
{

struct MergeTreeIndexReadResult;
using MergeTreeIndexReadResultPtr = std::shared_ptr<MergeTreeIndexReadResult>;

/// A reader used in the first reading step to apply index-based filtering. Currently, skip indexes are used to
/// determine which granules are relevant to the query, and only those are passed to subsequent readers.
class MergeTreeReaderIndex : public IMergeTreeReader
{
public:
    using MatchingMarks = std::vector<bool>;

    MergeTreeReaderIndex(const IMergeTreeReader * main_reader_, MergeTreeIndexReadResultPtr index_read_result_);

    size_t readRows(
        size_t from_mark,
        size_t current_task_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        size_t offset,
        Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return main_reader->canReadIncompleteGranules(); }

    bool canSkipMark(size_t mark, size_t current_task_last_mark) override;

private:
    /// Delegates to the main reader to determine if reading incomplete index granules is supported.
    const IMergeTreeReader * main_reader;

    /// Used to filter data during merge tree reading.
    MergeTreeIndexReadResultPtr index_read_result;

    /// Current row position used when continuing reads across multiple calls.
    size_t current_row = 0;
};

}
