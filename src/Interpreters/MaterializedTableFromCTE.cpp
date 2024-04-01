#include <atomic>
#include <Interpreters/MaterializedTableFromCTE.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::pair<std::unique_ptr<QueryPlan>, std::shared_future<bool>> FutureTableFromCTE::buildPlanOrGetPromiseToMaterialize(ContextPtr context)
{
    bool expected = false;
    QueryPlanPtr plan;

    if (get_permission_to_build_plan.compare_exchange_strong(expected, true))
    {
        const auto & settings = context->getSettingsRef();
        plan = std::move(source);

        if (!plan)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query plan to fill CTE must not be not NULL.");

        auto creating_set = std::make_unique<MaterializingCTEStep>(
                plan->getCurrentDataStream(),
                shared_from_this(),
                SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
                context);
        creating_set->setStepDescription("Create temporary table from CTE.");
        plan->addStep(std::move(creating_set));
    }

    return {std::move(plan), fully_materialized};
}

}
