#pragma once
#include <optional>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

/// Class that contains information about index granularity in rows of IMergeTreeDataPart
class MergeTreeIndexGranularity
{
public:
    MergeTreeIndexGranularity() = default;
    virtual ~MergeTreeIndexGranularity() = default;

    /// Returns granularity if it is constant for whole part (except last granule).
    virtual std::optional<size_t> getConstantGranularity() const = 0;
    /// Return count of rows between marks
    virtual size_t getRowsCountInRange(size_t begin, size_t end) const = 0;
    /// Return count of rows between marks
    size_t getRowsCountInRange(const MarkRange & range) const;
    /// Return sum of rows between all ranges
    size_t getRowsCountInRanges(const MarkRanges & ranges) const;

    /// Return number of marks, starting from `from_marks` that contain `number_of_rows`
    virtual size_t countMarksForRows(size_t from_mark, size_t number_of_rows) const = 0;

    /// Return number of rows, starting from `from_mark`, that contains amount of `number_of_rows`
    /// and possible some offset_in_rows from `from_mark`
    ///                                     1    2  <- answer
    /// |-----|---------------------------|----|----|
    ///       ^------------------------^-----------^
    ////  from_mark  offset_in_rows    number_of_rows
    virtual size_t countRowsForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows) const = 0;

    /// Total marks
    virtual size_t getMarksCount() const = 0;
    /// Total rows
    virtual size_t getTotalRows() const = 0;

    /// Total number marks without final mark if it exists
    size_t getMarksCountWithoutFinal() const;

    /// Rows after mark to next mark
    virtual size_t getMarkRows(size_t mark_index) const = 0;

    /// Return amount of rows before mark
    size_t getMarkStartingRow(size_t mark_index) const;

    /// Return the mark associated with the target row offset.
    virtual MarkRange getMarkRangeForRowOffset(size_t row_offset) const = 0;

    /// Amount of rows after last mark
    size_t getLastMarkRows() const;

    /// Amount of rows after last non-final mark
    size_t getLastNonFinalMarkRows() const;

    virtual bool hasFinalMark() const = 0;
    bool empty() const { return getMarksCount() == 0; }

    /// Add new mark with rows_count.
    virtual void appendMark(size_t rows_count) = 0;

    /// Sets last mark equal to rows_count.
    virtual void adjustLastMark(size_t rows_count) = 0;
    void addRowsToLastMark(size_t rows_count);

    virtual uint64_t getBytesSize() const = 0;
    virtual uint64_t getBytesAllocated() const = 0;

    /// Possibly optimizes values in memory (for example, to constant value).
    /// Returns new optimized index granularity structure or nullptr if no optimization is not applicable.
    virtual std::shared_ptr<MergeTreeIndexGranularity> optimize() = 0;
    virtual std::string describe() const = 0;
};

using MergeTreeIndexGranularityPtr = std::shared_ptr<MergeTreeIndexGranularity>;

size_t computeIndexGranularity(
    size_t rows,
    size_t bytes_uncompressed,
    size_t index_granularity_bytes,
    size_t fixed_index_granularity_rows,
    bool blocks_are_granules,
    bool can_use_adaptive_index_granularity);

struct MergeTreeSettings;
struct MergeTreeIndexGranularityInfo;

MergeTreeIndexGranularityPtr createMergeTreeIndexGranularity(
    size_t rows,
    size_t bytes_uncompressed,
    const MergeTreeSettings & settings,
    const MergeTreeIndexGranularityInfo & info,
    bool blocks_are_granules);

}
