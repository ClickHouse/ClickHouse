#pragma once

#include <Processors/Transforms/FilterTransform.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// Could be used when the predicate given by expression_ is true only on the one continuous range of input rows.
/// The following optimization applies: when a new chunk of data comes in, we firstly execute the expression_ only on the first and the last row -
/// if it evaluates to true on both rows then the whole chunk is immediately passed to further steps.
/// Otherwise, we apply the expression_ to all rows.
class FilterSortedStreamByRange : public ISimpleTransform
{
public:
    FilterSortedStreamByRange(
        SharedHeader header_,
        ExpressionActionsPtr expression_,
        String filter_column_name_,
        bool remove_filter_column_,
        bool on_totals_ = false);

    String getName() const override { return "FilterSortedStreamByRange"; }

    void transform(Chunk & chunk) override;

private:
    FilterTransform filter_transform;
};


}
