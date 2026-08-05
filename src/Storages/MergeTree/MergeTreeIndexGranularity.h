#pragma once
#include <optional>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

class Block;

/// Class that contains information about index granularity in rows of IMergeTreeDataPart
class MergeTreeIndexGranularity
{
public:
    MergeTreeIndexGranularity() = default;
    MergeTreeIndexGranularity(const MergeTreeIndexGranularity &) = default;
    MergeTreeIndexGranularity & operator=(const MergeTreeIndexGranularity &) = default;
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
    size_t getMarksCountForSkipIndex(size_t skip_index_granularity) const;

    virtual uint64_t getBytesSize() const = 0;
    virtual uint64_t getBytesAllocated() const = 0;

    /// Possibly optimizes values in memory (for example, to constant value).
    /// Returns new optimized index granularity structure or nullptr if no optimization is not applicable.
    virtual std::shared_ptr<MergeTreeIndexGranularity> optimize() = 0;
    virtual std::string describe() const = 0;

    /// Deep-copy, so a written part's granularity can be re-homed into the dedicated MergeTree arena.
    virtual std::shared_ptr<MergeTreeIndexGranularity> clone() const = 0;
};

using MergeTreeIndexGranularityPtr = std::shared_ptr<MergeTreeIndexGranularity>;

size_t computeIndexGranularity(
    size_t rows,
    size_t bytes_uncompressed,
    size_t index_granularity_bytes,
    size_t fixed_index_granularity_rows,
    bool blocks_are_granules,
    bool can_use_adaptive_index_granularity);

/// Uncompressed size of the block as it will be written into a data part, used to choose the index
/// granularity against index_granularity_bytes. Equal to Block::bytes() except for AggregateFunction
/// state columns, whose true size Block::bytes() cannot report (their states live in shared arenas
/// that ColumnAggregateFunction::byteSize() intentionally does not count).
size_t getBlockSizeForGranularity(const Block & block);

struct MergeTreeSettings;
struct MergeTreeIndexGranularityInfo;

MergeTreeIndexGranularityPtr createMergeTreeIndexGranularity(
    size_t rows,
    size_t bytes_uncompressed,
    const MergeTreeSettings & settings,
    const MergeTreeIndexGranularityInfo & info,
    bool blocks_are_granules);

/// Overload for the write path: computes the block size (which serializes AggregateFunction
/// states) only when the constant-granularity path will use it, avoiding a wasted sizing pass
/// on the non-const adaptive path where the per-block writer recomputes it anyway.
MergeTreeIndexGranularityPtr createMergeTreeIndexGranularity(
    const Block & block,
    const MergeTreeSettings & settings,
    const MergeTreeIndexGranularityInfo & info,
    bool blocks_are_granules);

}
