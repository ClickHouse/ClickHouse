#include <atomic>
#include <Interpreters/MaterializedTableFromCTE.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>

namespace DB
{

std::unique_ptr<QueryPlan> FutureTableFromCTE::build(ContextPtr context)
{
    bool expected_built = false;
    if (built.compare_exchange_strong(expected_built, true))
        return nullptr;

    const auto & settings = context->getSettingsRef();
    auto plan = std::move(source);

    if (!plan)
        return nullptr;

    auto creating_set = std::make_unique<MaterializingCTEStep>(
            plan->getCurrentDataStream(),
            external_table,
            name,
            SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
            context);
    creating_set->setStepDescription("Create temporary table from CTE.");
    plan->addStep(std::move(creating_set));
    return plan;
}

}
