#pragma once

#include <Common/PODArray_fwd.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/finalizeChunk.h>
#include <Interpreters/SetSpillable.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
using IColumnFilter = PaddedPODArray<UInt8>;

class ActionsDAG;

enum class TotalsMode : uint8_t;

/** Takes blocks after grouping, with non-finalized aggregate functions.
  * Calculates total values according to totals_mode.
  * If necessary, evaluates the expression from HAVING and filters rows. Returns the finalized and filtered blocks.
  */
class TotalsHavingTransform final : public ISimpleTransform
{
public:
    TotalsHavingTransform(
        const Block & header,
        const ColumnsMask & aggregates_mask_,
        bool overflow_row_,
        const ExpressionActionsPtr & expression_,
        const std::string & filter_column_,
        bool remove_filter_,
        TotalsMode totals_mode_,
        double auto_include_threshold_,
        bool final_);

    String getName() const override { return "TotalsHavingTransform"; }
    std::span<ISpillable * const> getSpillables() override { return set_spillables.get(); }

    OutputPort & getTotalsPort() { return outputs.back(); }

    Status prepare() override;
    void work() override;
    void onCancel() noexcept override;

    bool hasFilter() const { return !filter_column_name.empty(); }

    static Block transformHeader(Block block, const ActionsDAG * expression, const std::string & filter_column_name, bool remove_filter, bool final, const ColumnsMask & aggregates_mask);

protected:
    void transform(Chunk & chunk) override;

    bool finished_transform = false;
    bool total_prepared = false;
    Chunk totals;

private:
    void addToTotals(const Chunk & chunk, const IColumnFilter * filter);
    /// Merges the aggregate states of `columns` (only the rows passing `filter`, if it is set) into the
    /// single row of aggregate states in `target`.
    void mergeAggregateStates(MutableColumns & target, const Columns & columns, const IColumnFilter * filter);
    void prepareTotals();

    /// Params
    const ColumnsMask aggregates_mask;
    bool overflow_row;
    ExpressionActionsPtr expression;
    SetSpillables set_spillables;
    String filter_column_name;
    bool remove_filter;
    TotalsMode totals_mode;
    double auto_include_threshold;
    bool final;

    size_t passed_keys = 0;
    size_t total_keys = 0;

    size_t filter_column_pos = 0;

    Block finalized_header;

    /// Here are the values that did not pass max_rows_to_group_by, merged into a single row.
    /// They are added or not added to the current_totals, depending on the totals_mode.
    /// Normally the aggregator emits at most one overflow chunk, but with `group_by_each_block_no_merge`
    /// every input block can produce its own. Each of them is merged here as soon as it arrives instead of
    /// being kept, so the memory stays bounded by a single overflow state rather than one per block.
    MutableColumns overflow_aggregates;
    bool has_overflow_aggregates = false;

    /// Here, total values are accumulated. After the work is finished, they will be placed in totals.
    MutableColumns current_totals;
};

}
