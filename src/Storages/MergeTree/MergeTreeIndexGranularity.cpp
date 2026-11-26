#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Core/Block.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
    extern const MergeTreeSettingsBool use_const_adaptive_granularity;
}

size_t MergeTreeIndexGranularity::getRowsCountInRange(const MarkRange & range) const
{
    return getRowsCountInRange(range.begin, range.end);
}

size_t MergeTreeIndexGranularity::getRowsCountInRanges(const MarkRanges & ranges) const
{
    size_t total = 0;
    for (const auto & range : ranges)
        total += getRowsCountInRange(range);
    return total;
}

size_t MergeTreeIndexGranularity::getMarksCountWithoutFinal() const
{
    size_t total = getMarksCount();
    if (total == 0)
        return total;
    return total - hasFinalMark();
}

size_t MergeTreeIndexGranularity::getMarkStartingRow(size_t mark_index) const
{
    return getRowsCountInRange(0, mark_index);
}

size_t MergeTreeIndexGranularity::getLastMarkRows() const
{
    return getMarkRows(getMarksCount() - 1);
}

size_t MergeTreeIndexGranularity::getLastNonFinalMarkRows() const
{
    size_t last_mark_rows = getMarkRows(getMarksCount() - 1);
    if (last_mark_rows != 0)
        return last_mark_rows;
    return getMarkRows(getMarksCount() - 2);
}

void MergeTreeIndexGranularity::addRowsToLastMark(size_t rows_count)
{
    if (hasFinalMark())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add rows to final mark");
    }
    else if (empty())
    {
        appendMark(rows_count);
    }
    else
    {
        adjustLastMark(getLastMarkRows() + rows_count);
    }
}

/// Granularity size of one (possibly composite) column. byteSize() already sums nested children,
/// but under-reports ColumnAggregateFunction leaves (pointer-only), so for every such leaf at any
/// depth we add the serializedSizeEstimate()-vs-byteSize() delta. forEachSubcolumnRecursively does
/// not visit the column itself, hence the explicit top-level call. Saturating: guard underflow.
static size_t getColumnSizeForGranularity(const IColumn & column)
{
    size_t res = column.byteSize();

    auto add_agg_correction = [&](const IColumn & sub)
    {
        if (const auto * agg = typeid_cast<const ColumnAggregateFunction *>(&sub))
        {
            const size_t serialized = agg->serializedSizeEstimate();
            const size_t counted = agg->byteSize();
            if (serialized > counted)
                res += serialized - counted;
        }
    };

    add_agg_correction(column);
    column.forEachSubcolumnRecursively(add_agg_correction);

    return res;
}

size_t getBlockSizeForGranularity(const Block & block)
{
    size_t res = 0;
    for (const auto & elem : block)
        if (elem.column)
            res += getColumnSizeForGranularity(*elem.column);
    return res;
}

size_t computeIndexGranularity(
    size_t rows,
    size_t bytes_uncompressed,
    size_t index_granularity_bytes,
    size_t fixed_index_granularity_rows,
    bool blocks_are_granules,
    bool can_use_adaptive_index_granularity)
{
    size_t index_granularity_for_block = 0;

    if (!can_use_adaptive_index_granularity)
    {
        index_granularity_for_block = fixed_index_granularity_rows;
    }
    else
    {
        if (blocks_are_granules)
        {
            index_granularity_for_block = rows;
        }
        else if (index_granularity_bytes != 0 && bytes_uncompressed >= index_granularity_bytes)
        {
            /// index_granularity_bytes == 0 disables adaptive sizing upstream, so this branch is
            /// unreachable with a zero divisor; the explicit check makes that visible to static analysis.
            size_t granules_in_block = bytes_uncompressed / index_granularity_bytes;
            index_granularity_for_block = rows / granules_in_block;
        }
        else
        {
            size_t size_of_row_in_bytes = std::max(bytes_uncompressed / rows, 1UL);
            index_granularity_for_block = index_granularity_bytes / size_of_row_in_bytes;
        }
    }

    /// We should be less or equal than fixed index granularity.
    /// But if block size is a granule size then do not adjust it.
    /// Granularity greater than fixed granularity might come from compact part.
    if (!blocks_are_granules)
        index_granularity_for_block = std::min(fixed_index_granularity_rows, index_granularity_for_block);

    /// Very rare case when index granularity bytes less than single row.
    if (index_granularity_for_block == 0)
        index_granularity_for_block = 1;

    return index_granularity_for_block;
}

/// Whether the block-uncompressed-bytes figure is actually used to pick the granularity.
/// It is ignored (a plain adaptive object is returned) for empty/blocks-as-granules/compact
/// parts and for the non-const adaptive path. Callers use this to avoid computing an
/// expensive size (getBlockSizeForGranularity serializes AggregateFunction states) that
/// would be thrown away.
static bool constantGranularityUsesBytes(
    size_t rows,
    const MergeTreeSettings & settings,
    const MergeTreeIndexGranularityInfo & info,
    bool blocks_are_granules)
{
    bool use_adaptive_granularity = info.mark_type.adaptive;
    bool use_const_adaptive_granularity = settings[MergeTreeSetting::use_const_adaptive_granularity];
    bool is_compact_part = info.mark_type.part_type == MergeTreeDataPartType::Compact;

    /// Compact parts cannot work without adaptive granularity.
    /// If part is empty create adaptive granularity because constant granularity doesn't support this corner case.
    return !(rows == 0 || blocks_are_granules || is_compact_part || (use_adaptive_granularity && !use_const_adaptive_granularity));
}

MergeTreeIndexGranularityPtr createMergeTreeIndexGranularity(
    size_t rows,
    size_t bytes_uncompressed,
    const MergeTreeSettings & settings,
    const MergeTreeIndexGranularityInfo & info,
    bool blocks_are_granules)
{
    if (!constantGranularityUsesBytes(rows, settings, info, blocks_are_granules))
        return std::make_shared<MergeTreeIndexGranularityAdaptive>();

    size_t computed_granularity = computeIndexGranularity(
        rows,
        bytes_uncompressed,
        settings[MergeTreeSetting::index_granularity_bytes],
        settings[MergeTreeSetting::index_granularity],
        blocks_are_granules,
        info.mark_type.adaptive);

    return std::make_shared<MergeTreeIndexGranularityConstant>(computed_granularity);
}

MergeTreeIndexGranularityPtr createMergeTreeIndexGranularity(
    const Block & block,
    const MergeTreeSettings & settings,
    const MergeTreeIndexGranularityInfo & info,
    bool blocks_are_granules)
{
    /// Only size the block (which serializes variable-size AggregateFunction states) when the
    /// constant-granularity path will actually consume it. On the non-const adaptive path the
    /// figure is ignored here and recomputed per block by MergeTreeDataPartWriterOnDisk, so
    /// computing it now would be a wasted serialization pass.
    if (!constantGranularityUsesBytes(block.rows(), settings, info, blocks_are_granules))
        return std::make_shared<MergeTreeIndexGranularityAdaptive>();

    return createMergeTreeIndexGranularity(block.rows(), getBlockSizeForGranularity(block), settings, info, blocks_are_granules);
}

size_t MergeTreeIndexGranularity::getMarksCountForSkipIndex(size_t skip_index_granularity) const
{
    size_t marks_count = getMarksCountWithoutFinal();
    return (marks_count + skip_index_granularity - 1) / skip_index_granularity;
}

}
