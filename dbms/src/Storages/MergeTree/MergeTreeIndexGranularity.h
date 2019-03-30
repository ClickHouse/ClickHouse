#pragma once
#include <vector>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

class MergeTreeIndexGranularity
{
private:
    std::vector<size_t> marks_rows_partial_sums;
    bool initialized = false;

public:
    MergeTreeIndexGranularity() = default;
    explicit MergeTreeIndexGranularity(const std::vector<size_t> & marks_rows_partial_sums_);
    MergeTreeIndexGranularity(size_t marks_count, size_t fixed_granularity);


    size_t getRowsCountInRange(const MarkRange & range) const;
    size_t getRowsCountInRange(size_t begin, size_t end) const;
    size_t getRowsCountInRanges(const MarkRanges & ranges) const;
    size_t getMarkPositionInRows(const size_t mark_index) const;

    size_t countMarksForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows=0) const;

    size_t getAvgGranularity() const;
    size_t getMarksCount() const;
    size_t getTotalRows() const;

    inline size_t getMarkRows(size_t mark_index) const
    {
        if (mark_index == 0)
            return marks_rows_partial_sums[0];
        else
            return marks_rows_partial_sums[mark_index] - marks_rows_partial_sums[mark_index - 1];
    }

    size_t getMarkStartingRow(size_t mark_index) const;

    size_t getLastMarkRows() const
    {
        size_t last = marks_rows_partial_sums.size() - 1;
        return getMarkRows(last);
    }

    bool empty() const
    {
        return marks_rows_partial_sums.empty();
    }
    bool isInitialized() const
    {
        return initialized;
    }

    void setInitialized()
    {
        initialized = true;
    }
    void appendMark(size_t rows_count);
    void resizeWithFixedGranularity(size_t size, size_t fixed_granularity);
};

}
