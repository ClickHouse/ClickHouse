#include <QueryPipeline/PipelineResourcesHolder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryIdHolder.h>

namespace DB
{

QueryPlanResourceHolder & QueryPlanResourceHolder::operator=(QueryPlanResourceHolder && rhs) noexcept
{
    table_locks.insert(table_locks.end(), rhs.table_locks.begin(), rhs.table_locks.end());
    storage_holders.insert(storage_holders.end(), rhs.storage_holders.begin(), rhs.storage_holders.end());
    interpreter_context.insert(interpreter_context.end(),
                               rhs.interpreter_context.begin(), rhs.interpreter_context.end());
    query_id_holders.insert(query_id_holders.end(), rhs.query_id_holders.begin(), rhs.query_id_holders.end());

    return *this;
}

QueryPlanResourceHolder::QueryPlanResourceHolder() = default;
QueryPlanResourceHolder::QueryPlanResourceHolder(QueryPlanResourceHolder &&) noexcept = default;
QueryPlanResourceHolder::~QueryPlanResourceHolder() = default;

}
