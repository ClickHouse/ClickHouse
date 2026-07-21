#pragma once

#include <memory>
#include <vector>

#include <Columns/IColumn.h>
#include <Core/Block.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// A residual JOIN ON condition beyond the conditions a specialized join operator executes
/// itself, evaluated per candidate pair: a single-output boolean expression over columns of
/// both inputs. A pair matches only when the operator's own conditions hold AND the residual
/// passes (a NULL result counts as failed).
struct JoinResidualCondition
{
    /// Where a required column of `actions` comes from: the input (0 = left, 1 = right)
    /// and the column's position in that input's header.
    struct Source
    {
        size_t side = 0;
        size_t position = 0;
    };

    ExpressionActionsPtr actions;
    /// One entry per required column of `actions`, in `getRequiredColumnsWithTypes` order.
    std::vector<Source> inputs;
};

/// Resolve the required columns of a single-boolean-output expression against the two input
/// headers into the {side, position} sources; every column must come from exactly one input.
JoinResidualCondition resolveJoinResidualCondition(
    ExpressionActionsPtr actions, const Block & left_header, const Block & right_header);

/// The residual bound to concrete input headers: validates the sources against them and
/// precomputes what `ExpressionActions::executeOnColumns` needs. The operator gathers the
/// input columns per candidate pair (one column per `sources()` entry, in order) - how the
/// pairs are addressed is the operator's business - and `evaluateMask` folds the expression
/// result into a byte mask over the candidates, a NULL result counting as failed.
class JoinResidualConditionEvaluator
{
public:
    JoinResidualConditionEvaluator(
        JoinResidualCondition condition_, const Block & left_header, const Block & right_header);

    const std::vector<JoinResidualCondition::Source> & sources() const { return condition.inputs; }
    const ExpressionActionsPtr & actions() const { return condition.actions; }

    /// `columns` are the gathered inputs, all of `num_rows` rows; 1 = the pair passes.
    IColumn::Filter evaluateMask(Columns columns, size_t num_rows) const;

private:
    JoinResidualCondition condition;
    /// Header of the input columns (in required-columns order) and the precomputed input
    /// positions for `ExpressionActions::executeOnColumns`.
    Block input_header;
    std::vector<ssize_t> input_positions;
};

}
