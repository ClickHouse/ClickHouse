#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Implementations of a final `DISTINCT`:
///   - Local: the whole input on one node (always applicable);
///   - Shuffle: the input split among the nodes by the `DISTINCT` columns, by all of them or by a single one,
///     so equal rows meet on one node and each node deduplicates its own share of the values.
///
/// The preliminary `DISTINCT` below it deduplicates each stream on its own, so it runs wherever its input is
/// (see `isDistributionPassthroughStep`): before a gather it deduplicates on every node and the gather moves
/// the distinct rows only. A `DISTINCT` emits a row on its first occurrence, so both strategies keep streaming
/// and the `LIMIT` above can stop it early.
class DistinctImplementation : public IOptimizationRule
{
public:
    [[nodiscard]] String getName() const override { return "Distinct"; }

    [[nodiscard]] bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const override
    {
        const auto * step = typeid_cast<const DistinctStep *>(expression->getQueryPlanStep());
        return step && !step->isPreliminary() && expression->strategy == nullptr;
    }

    [[nodiscard]] Promise getPromise() const override { return 3000; }
    [[nodiscard]] bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override
    {
        const auto & step = *typeid_cast<const DistinctStep *>(expression->getQueryPlanStep());
        std::vector<GroupExpressionPtr> result;

        auto add_alternative = [&](ImplementationStrategyPtr strategy, const DistributionDescription & distribution, String strategy_name)
        {
            auto new_step = step.clone();
            const auto & description = step.getStepDescription();
            new_step->setStepDescription(description.empty() ? std::move(strategy_name) : fmt::format("{} {}", strategy_name, description), 200);
            auto alternative = std::make_shared<GroupExpression>(*expression);
            alternative->plan_step = std::move(new_step);
            alternative->strategy = std::move(strategy);
            alternative->inputs[0].required_properties.distribution = distribution;
            alternative->properties.distribution = distribution;
            addPhysicalToMemo(alternative, required_properties, memo, result);
        };

        add_alternative(strategySingleton<LocalDistinctStrategy>(), DistributionDescription{}, "Local");

        /// A `DISTINCT` that keeps the order of its input stays on one node: the shares of the nodes would
        /// interleave when gathered. So does one with `max_rows_in_distinct` or `max_bytes_in_distinct`: each
        /// node would check the limit on its own share only, and a result over the limit would pass.
        const auto & columns = step.getColumnNames();
        const auto & size_limits = step.getSettings().set_size_limits;
        const bool has_size_limits = size_limits.max_rows != 0 || size_limits.max_bytes != 0;
        if (step.preservesInputOrder() || has_size_limits || columns.empty())
            return result;

        for (size_t node_count : getCandidateNodeCounts(memo.getContext().cluster_node_count))
        {
            DistributionDescription by_all_columns;
            by_all_columns.node_count = node_count;
            for (const auto & column : columns)
                by_all_columns.columns.push_back({column});
            add_alternative(strategySingleton<ShuffleDistinctStrategy>(), by_all_columns, "Shuffle");

            /// Rows equal in all the columns are equal in any one of them, so a split by one column also
            /// brings equal rows together. It lets the input keep a split it already has, e.g. by a join key.
            if (columns.size() < 2)
                continue;
            for (const auto & column : columns)
            {
                DistributionDescription by_one_column;
                by_one_column.node_count = node_count;
                by_one_column.columns.push_back({column});
                add_alternative(strategySingleton<ShuffleDistinctStrategy>(), by_one_column, fmt::format("Shuffle (by {})", column));
            }
        }
        return result;
    }
};

OptimizationRulePtr createDistinctImplementation() { return std::make_shared<DistinctImplementation>(); }

}
