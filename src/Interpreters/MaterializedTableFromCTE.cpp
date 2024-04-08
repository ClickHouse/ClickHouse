#include <atomic>
#include <Interpreters/MaterializedTableFromCTE.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/Sinks/EmptySink.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>

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

bool materializeFutureTablesIfNeeded(ContextPtr context, const FutureTablesFromCTE & required_future_tables)
{
    std::vector<std::shared_future<bool>> promise_to_materialize_future_tables;
    QueryPipelineBuilders pipeline_to_build_future_tables;
    for (const auto & future_table : required_future_tables)
    {
        auto [plan, promise] = future_table->buildPlanOrGetPromiseToMaterialize(context);
        if (plan)
            pipeline_to_build_future_tables.emplace_back(plan->buildQueryPipeline(
                QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context)));

        promise_to_materialize_future_tables.push_back(std::move(promise));
    }

    if (!pipeline_to_build_future_tables.empty())
    {
        QueryPipelineBuilder builder;
        if (pipeline_to_build_future_tables.size() > 1)
            builder = QueryPipelineBuilder::unitePipelines(
                std::move(pipeline_to_build_future_tables), context->getSettingsRef().max_threads, nullptr);
        else
            builder = std::move(*pipeline_to_build_future_tables.front());

        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
        pipeline.complete(std::make_shared<EmptySink>(Block()));
        CompletedPipelineExecutor executor(pipeline);
        executor.execute();
    }

    bool all_table_fully_materialized = true;
    for (auto & promise : promise_to_materialize_future_tables)
    {
        promise.wait();
        if (const auto & materialized = promise.get(); !materialized)
            all_table_fully_materialized = false;
    }

    return all_table_fully_materialized;
}

}
