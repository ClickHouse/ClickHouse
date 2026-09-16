#pragma once
#include <Columns/FilterDescription.h>
#include <Core/ColumnNumbers.h>
#include <Core/SortDescription.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

/** Drops rows that cannot pass a `rank() <= top_k` / `row_number() <= top_k` filter placed above the
  * window, before the window's sort sees them.
  *
  * Soundness. Within one chunk, for each PARTITION BY key, a heap holds the `top_k` best ORDER BY keys
  * seen so far in that chunk. A row is dropped only when the heap already holds `top_k` values strictly
  * better than the row's own, each belonging to a distinct earlier row of the same partition; the row's
  * final rank is therefore at least `top_k + 1`, whatever the rest of the input looks like. A row tying
  * with the heap's worst entry is always forwarded, which is what makes this correct for `rank()` (all
  * rows of a tie block share one rank) rather than for a plain row count. `row_number() >= rank()`, so a
  * `row_number()` bound is covered by the same argument. Every row within the bound has every row of its
  * partition ranked ahead of it forwarded too, so the ranks the window computes for the rows the filter
  * keeps are exact. A row forwarded with a rank already past the bound may be ranked lower than it would
  * have been, which is what the filter above discards.
  *
  * The argument only ever uses "rows of this partition that this instance has already seen", and a chunk
  * is such a subset just as much as a whole stream is - so no state has to survive `transform`, and the
  * transform may run per stream before the scatter, with no exchange: each stream forwards a superset of
  * what the filter above keeps.
  */
class WindowTopKPrefilterTransform final : public ISimpleTransform
{
public:
    WindowTopKPrefilterTransform(
        SharedHeader header_,
        const SortDescription & partition_description_,
        const SortDescription & order_description_,
        UInt64 top_k_);

    String getName() const override { return "WindowTopKPrefilterTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    const SortDescription partition_description;
    const SortDescription order_description;
    const UInt64 top_k;

    ColumnNumbers partition_positions;
    ColumnNumbers order_positions;

    /// Rows to observe before the skip rate may freeze the transform into a pass-through, and the
    /// counters it is judged on.
    const UInt64 profitability_window;
    UInt64 observed_rows = 0;
    UInt64 skipped_rows = 0;
    bool frozen = false;

    IColumnFilter filter;
};

}
