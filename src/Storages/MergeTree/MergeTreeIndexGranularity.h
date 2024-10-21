#pragma once
#include <vector>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

/// Class contains information about index granularity in rows of IMergeTreeDataPart
/// Inside it contains vector of partial sums of rows after mark:
/// |-----|---|----|----|
/// |  5  | 8 | 12 | 16 |
/// If user doesn't specify setting index_granularity_bytes for MergeTree* table
/// all values in inner vector would have constant stride (default 8192).
class MergeTreeIndexGranularity
{
private:
    std::vector<size_t> marks_rows_partial_sums;
    bool initialized = false;

public:
    MergeTreeIndexGranularity() = default;
    explicit MergeTreeIndexGranularity(const std::vector<size_t> & marks_rows_partial_sums_);

    /// Return count of rows between marks
    size_t getRowsCountInRange(const MarkRange & range) const;
    /// Return count of rows between marks
    size_t getRowsCountInRange(size_t begin, size_t end) const;
    /// Return sum of rows between all ranges
    size_t getRowsCountInRanges(const MarkRanges & ranges) const;

    /// Return number of marks, starting from `from_marks` that contain `number_of_rows`
    size_t countMarksForRows(size_t from_mark, size_t number_of_rows) const;

    /// Return number of rows, starting from `from_mark`, that contains amount of `number_of_rows`
    /// and possible some offset_in_rows from `from_mark`
    ///                                     1    2  <- answer
    /// |-----|---------------------------|----|----|
    ///       ^------------------------^-----------^
    ////  from_mark  offset_in_rows    number_of_rows
    size_t countRowsForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows) const;

    /// Total marks
    size_t getMarksCount() const;
    /// Total rows
    size_t getTotalRows() const;

    /// Total number marks without final mark if it exists
    size_t getMarksCountWithoutFinal() const { return getMarksCount() - hasFinalMark(); }

    /// Rows after mark to next mark
    size_t getMarkRows(size_t mark_index) const;

    /// Return amount of rows before mark
    size_t getMarkStartingRow(size_t mark_index) const;

    /// Amount of rows after last mark
    size_t getLastMarkRows() const
    {
        size_t last = marks_rows_partial_sums.size() - 1;
        return getMarkRows(last);
    }

    size_t getLastNonFinalMarkRows() const
    {
        size_t last_mark_rows = getLastMarkRows();
        if (last_mark_rows != 0)
            return last_mark_rows;
        return getMarkRows(marks_rows_partial_sums.size() - 2);
    }

    bool hasFinalMark() const
    {
        return getLastMarkRows() == 0;
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
    /// Add new mark with rows_count
    void appendMark(size_t rows_count);

    /// Extends last mark by rows_count.
    void addRowsToLastMark(size_t rows_count);

    /// Drops last mark if any exists.
    void popMark();

    /// Add `size` of marks with `fixed_granularity` rows
    void resizeWithFixedGranularity(size_t size, size_t fixed_granularity);

    std::string describe() const;
};

}
