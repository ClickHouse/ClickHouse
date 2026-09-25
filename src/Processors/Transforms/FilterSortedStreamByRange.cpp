#include <Processors/Transforms/FilterSortedStreamByRange.h>

#include <Columns/IColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/FailPoint.h>

namespace DB
{

namespace FailPoints
{
    extern const char filter_sorted_stream_by_range_pause[];
    extern const char filter_sorted_stream_by_range_fallback_pause[];
}

namespace
{

void markRangesInfoDroppedRows(Chunk & chunk)
{
    auto mark_ranges_info = chunk.getChunkInfos().extract<MarkRangesInfo>();
    if (!mark_ranges_info)
        return;

    /// `ChunkInfo` objects are held by `shared_ptr` and may be aliased: `get` returns the pointer
    /// itself and a collection copy shares it. Follow the copy-on-write convention instead of
    /// mutating in place.
    auto updated_info = std::static_pointer_cast<MarkRangesInfo>(mark_ranges_info->clone());
    updated_info->has_dropped_rows = true;
    chunk.getChunkInfos().add(std::move(updated_info));
}

}

FilterSortedStreamByRange::FilterSortedStreamByRange(
    SharedHeader header_, ExpressionActionsPtr expression_, String filter_column_name_, bool remove_filter_column_, bool on_totals_)
    : ISimpleTransform(
          header_,
          std::make_shared<const Block>(
              FilterTransform::transformHeader(*header_, &expression_->getActionsDAG(), filter_column_name_, remove_filter_column_)),
          true)
    , filter_transform(header_, expression_, filter_column_name_, remove_filter_column_, on_totals_)
{
    assertBlocksHaveEqualStructure(
        *header_, getOutputPort().getHeader(), "Expression for FilterSortedStreamByRange should not change header");
}

void FilterSortedStreamByRange::onCancel() noexcept
{
    ISimpleTransform::onCancel();
    /// The pipeline cancels only this outer processor, so forward the cancellation
    /// into the inner FilterTransform to interrupt long-running functions in its expression.
    filter_transform.cancel();
}

bool FilterSortedStreamByRange::stopIfCancelled(Chunk & chunk)
{
    if (!isCancelled())
        return false;

    chunk.clear();
    stopReading();
    return true;
}

void FilterSortedStreamByRange::transform(Chunk & chunk)
{
    /// A task that was already dispatched by the executor when the cancellation landed still runs,
    /// so a chunk buffered in the input port can still arrive here after `KILL QUERY`. Do not enter
    /// the filter expression while cancelled: drop the chunk and stop reading.
    if (stopIfCancelled(chunk))
        return;

    /// Pauses every entry that passed the cancellation guard above. Tests arm it after `KILL QUERY`,
    /// so a transform re-entered after the cancellation blocks here and the test times out.
    FailPointInjection::pauseFailPoint(FailPoints::filter_sorted_stream_by_range_pause);

    /// The cancellation can land after the guard above, so check it again right before delegating to
    /// the inner transform: the inner `FilterTransform` observes the cancellation only once it is
    /// already inside `ExpressionActions::execute`, so without this the killed query would evaluate
    /// at least one action of the filter expression for this chunk.
    if (stopIfCancelled(chunk))
        return;

    const UInt64 rows_before_filtration = chunk.getNumRows();
    if (rows_before_filtration < 2)
    {
        filter_transform.transform(chunk);
        /// The query was killed during the evaluation: the inner transform returned an empty chunk,
        /// and this outer transform must stop pulling further input.
        if (stopIfCancelled(chunk))
            return;
        if (chunk.getNumRows() != rows_before_filtration)
            markRangesInfoDroppedRows(chunk);
        return;
    }

    // Evaluate expression on just the first and the last row.
    // If both of them satisfies conditions, then skip calculation for all the rows in between.
    auto quick_check_columns = chunk.cloneEmptyColumns();
    auto src_columns = chunk.detachColumns();
    for (auto row : {static_cast<UInt64>(0), rows_before_filtration - 1})
    {
        for (size_t col = 0; col < quick_check_columns.size(); ++col)
            quick_check_columns[col]->insertFrom(*src_columns[col].get(), row);
    }
    chunk.setColumns(std::move(quick_check_columns), 2);
    filter_transform.transform(chunk);

    /// The query was killed while the probe was being evaluated: the inner transform returned
    /// an empty chunk, which must not be mistaken for a failed quick check — that would re-run
    /// the full expression on the whole chunk just to unwind. Return an empty chunk instead.
    if (stopIfCancelled(chunk))
        return;

    const bool all_rows_will_pass_filter = chunk.getNumRows() == 2;

    chunk.setColumns(std::move(src_columns), rows_before_filtration);

    // Not all rows satisfy conditions.
    if (!all_rows_will_pass_filter)
    {
        FailPointInjection::pauseFailPoint(FailPoints::filter_sorted_stream_by_range_fallback_pause);

        /// Same as before the probe: the cancellation can land while the range predicate of the
        /// probe is being evaluated (or while this thread is paused above), and the fallback call
        /// re-evaluates the predicate for the whole chunk — do not start it when cancelled.
        if (stopIfCancelled(chunk))
            return;

        filter_transform.transform(chunk);
        /// Same as above: a kill during the full evaluation must not leave a partially processed
        /// chunk in the output, and must stop this outer transform from pulling further input.
        if (stopIfCancelled(chunk))
            return;
        if (chunk.getNumRows() != rows_before_filtration)
            markRangesInfoDroppedRows(chunk);
    }
}

}
