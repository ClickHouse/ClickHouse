#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryIdHolder.h>

namespace DB
{

QueryPlanResourceHolder::QueryPlanResourceHolder() = default;
QueryPlanResourceHolder::QueryPlanResourceHolder(QueryPlanResourceHolder &&) noexcept = default;
QueryPlanResourceHolder & QueryPlanResourceHolder::operator=(QueryPlanResourceHolder && rhs) = default;
QueryPlanResourceHolder::~QueryPlanResourceHolder() = default;

void QueryPlanResourceHolder::add(QueryPlanResourceHolder && rhs) noexcept
{
    table_locks.insert(table_locks.end(), rhs.table_locks.begin(), rhs.table_locks.end());
    rhs.table_locks.clear();

    storage_holders.insert(storage_holders.end(), rhs.storage_holders.begin(), rhs.storage_holders.end());
    rhs.storage_holders.clear();

    interpreter_context.insert(interpreter_context.end(), rhs.interpreter_context.begin(), rhs.interpreter_context.end());
    rhs.interpreter_context.clear();

    query_id_holders.insert(query_id_holders.end(), rhs.query_id_holders.begin(), rhs.query_id_holders.end());
    rhs.query_id_holders.clear();
}

}
