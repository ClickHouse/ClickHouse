#pragma once
#include <vector>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

class IndexGranularity
{
private:
    std::vector<size_t> marks_to_rows;
    bool initialized = false;

public:
    IndexGranularity() = default;
    explicit IndexGranularity(const std::vector<size_t> & marks_to_rows_);
    IndexGranularity(size_t marks_count, size_t fixed_granularity);


    size_t getRowsCountInRange(const MarkRange & range) const;
    size_t getRowsCountInRanges(const MarkRanges & ranges) const;
    size_t getMarkPositionInRows(const size_t mark_index) const;


    size_t getAvgGranularity() const;
    size_t getMarksCount() const;
    size_t getTotalRows() const;
    inline size_t getMarkRows(size_t mark_index) const
    {
        if (mark_index == 0)
            return marks_to_rows[0];
        else
            return marks_to_rows[mark_index] - marks_to_rows[mark_index - 1];
    }
    size_t getMarkStartingRow(size_t mark_index) const;
    size_t getLastMarkRows() const
    {
        size_t last = marks_to_rows.size() - 1;
        return getMarkRows(last);
    }
    bool empty() const
    {
        return marks_to_rows.empty();
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
