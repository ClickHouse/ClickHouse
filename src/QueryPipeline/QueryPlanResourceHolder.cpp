#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

QueryPlanResourceHolder & QueryPlanResourceHolder::append(QueryPlanResourceHolder && rhs) noexcept
{
    table_locks.insert(table_locks.end(),
                       std::make_move_iterator(rhs.table_locks.begin()),
                       std::make_move_iterator(rhs.table_locks.end()));
    storage_holders.insert(storage_holders.end(),
                          std::make_move_iterator(rhs.storage_holders.begin()),
                          std::make_move_iterator(rhs.storage_holders.end()));
    interpreter_context.insert(interpreter_context.end(),
                               std::make_move_iterator(rhs.interpreter_context.begin()),
                               std::make_move_iterator(rhs.interpreter_context.end()));
    query_id_holders.insert(query_id_holders.end(),
                           std::make_move_iterator(rhs.query_id_holders.begin()),
                           std::make_move_iterator(rhs.query_id_holders.end()));
    insert_dependencies_holders.insert(insert_dependencies_holders.end(),
                                       std::make_move_iterator(rhs.insert_dependencies_holders.begin()),
                                       std::make_move_iterator(rhs.insert_dependencies_holders.end()));

    return *this;
}

QueryPlanResourceHolder & QueryPlanResourceHolder::append(const QueryPlanResourceHolder & rhs) noexcept
{
    table_locks.insert(table_locks.end(), rhs.table_locks.begin(), rhs.table_locks.end());
    storage_holders.insert(storage_holders.end(), rhs.storage_holders.begin(), rhs.storage_holders.end());
    interpreter_context.insert(interpreter_context.end(),
                               rhs.interpreter_context.begin(), rhs.interpreter_context.end());
    query_id_holders.insert(query_id_holders.end(), rhs.query_id_holders.begin(), rhs.query_id_holders.end());
    insert_dependencies_holders.insert(insert_dependencies_holders.end(), rhs.insert_dependencies_holders.begin(), rhs.insert_dependencies_holders.end());

    return *this;
}

QueryPlanResourceHolder & QueryPlanResourceHolder::operator=(QueryPlanResourceHolder && rhs) noexcept
{
    append(std::move(rhs));
    return *this;
}

QueryPlanResourceHolder::QueryPlanResourceHolder() = default;
QueryPlanResourceHolder::QueryPlanResourceHolder(QueryPlanResourceHolder &&) noexcept = default;
QueryPlanResourceHolder::~QueryPlanResourceHolder() = default;

}
