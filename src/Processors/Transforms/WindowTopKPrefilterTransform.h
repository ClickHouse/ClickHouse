#pragma once
#include <Columns/FilterDescription.h>
#include <Core/ColumnNumbers.h>
#include <Core/SortDescription.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

/** Drops rows that cannot pass a `rank() <= top_k` / `row_number() <= top_k` filter above the window.
  *
  * Soundness. A row is dropped only when `top_k` distinct rows of the same PARTITION BY key, already seen
  * by this instance, are strictly better in the window's ORDER BY: its final rank is then at least
  * `top_k + 1` whatever the rest of the input holds. A row tying with the heap's worst entry is forwarded,
  * which is what makes this correct for `rank()`, where one rank covers a whole tie block, rather than for
  * a row count; `row_number() >= rank()`, so a `row_number()` bound follows. Every row within the bound
  * keeps every row of its partition ranked ahead of it, so the kept rows' ranks are exact.
  *
  * The argument uses only "rows of this partition this instance has already seen", and a chunk is such a
  * subset just as much as a whole stream is: no state survives `transform`, and the transform may run per
  * stream before the scatter with no exchange, each stream forwarding a superset of what the filter keeps.
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

    /// Rows to observe before the skip rate may freeze the transform into a pass-through.
    const UInt64 profitability_window;
    UInt64 observed_rows = 0;
    UInt64 skipped_rows = 0;
    bool frozen = false;

    IColumnFilter filter;
};

}
