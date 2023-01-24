#include <QueryPipeline/PipelineResourcesHolder.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

PipelineResourcesHolder::PipelineResourcesHolder() = default;
PipelineResourcesHolder::PipelineResourcesHolder(PipelineResourcesHolder &&) noexcept = default;
PipelineResourcesHolder::~PipelineResourcesHolder() = default;

PipelineResourcesHolder & PipelineResourcesHolder::operator=(PipelineResourcesHolder && rhs) noexcept
{
    table_locks.insert(table_locks.end(), rhs.table_locks.begin(), rhs.table_locks.end());
    storage_holders.insert(storage_holders.end(), rhs.storage_holders.begin(), rhs.storage_holders.end());
    interpreter_context.insert(interpreter_context.end(),
                               rhs.interpreter_context.begin(), rhs.interpreter_context.end());
    for (auto & plan : rhs.query_plans)
        query_plans.emplace_back(std::move(plan));

    query_id_holder = std::move(rhs.query_id_holder);

    return *this;
}

}
