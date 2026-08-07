#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Parsers/IAST.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/DistributedPlanSets.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Core/Settings.h>
#include <Common/Exception.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_transfer;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

String describeSet(const FutureSetFromSubquery & future_set)
{
    if (auto ast = future_set.getSourceAST())
        return ast->formatForErrorMessage();
    const auto hash = future_set.getHash();
    return fmt::format("with hash {}_{}", hash.low64, hash.high64);
}

}

void validateSetsForDistributedPlan(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> stack;
    stack.push_back(&root);
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();
        if (!node || !node->step)
            continue;

        if (const auto * delayed = typeid_cast<const DelayedCreatingSetsStep *>(node->step.get()))
        {
            for (const auto & future_set : delayed->getSets())
            {
                if (future_set && future_set->hasExternalTable())
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "make_distributed_plan does not support sets backed by an external table "
                        "(`GLOBAL IN` / `GLOBAL JOIN`): IN-subquery {}", describeSet(*future_set));
            }
        }

        for (auto * child : node->children)
            stack.push_back(child);
    }
}

PreparedSets::Subqueries extractSetsForDistributedPlan(QueryPlan::Node *& root)
{
    PreparedSets::Subqueries sets;

    /// `DelayedCreatingSetsStep` is a transparent placeholder (its output header equals its input
    /// header), so after its sets are taken the node is spliced out of the tree.
    auto take_sets_and_splice_out = [&](QueryPlan::Node *& slot)
    {
        while (auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(slot->step.get()))
        {
            auto step_sets = delayed->detachSets();
            std::move(step_sets.begin(), step_sets.end(), std::back_inserter(sets));
            slot = slot->children.front();
        }
    };

    take_sets_and_splice_out(root);

    std::vector<QueryPlan::Node *> stack;
    stack.push_back(root);
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();
        for (auto & child : node->children)
        {
            take_sets_and_splice_out(child);
            stack.push_back(child);
        }
    }

    return sets;
}

void convertSetSourceForDistributedPlan(QueryPlan & source_plan, const ContextPtr & context)
{
    /// A source with a step that cannot be serialized for remote execution (e.g. a read from a
    /// table function) builds the set locally on the initiator instead of failing the query.
    if (findNonSerializableStep(source_plan.getRootNode(), /*ignore=*/ {}))
        return;

    /// The row limit makes an over-limit set fail during the build. The byte limit is not
    /// checked here: `DistinctStep` would measure its hash table rather than the shipped
    /// values, so task serialization checks it against the actual columns. A truncated set
    /// would change results, so the mode is throw.
    const auto & settings = context->getSettingsRef();
    SizeLimits transfer_limits(settings[Setting::max_rows_to_transfer], 0, OverflowMode::THROW);

    auto header = source_plan.getCurrentHeader();
    source_plan.addStep(
        std::make_unique<DistinctStep>(header, transfer_limits, 0, header->getNames(), /*pre_distinct_=*/false));

    QueryPlanOptimizationSettings optimization_settings(context);
    source_plan.optimize(optimization_settings);
    source_plan.convertToDistributed(optimization_settings);
}

}
