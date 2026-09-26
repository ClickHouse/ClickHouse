#include <Interpreters/MaterializedCTE.h>

#include <Common/thread_local_rng.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

MaterializedCTE::MaterializedCTE(const std::string & cte_name_)
    : cte_name(cte_name_)
    , temporary_table_name(fmt::format("_materialized_cte_{}_{}", cte_name, thread_local_rng()))
{}

MaterializedCTE::~MaterializedCTE() noexcept = default;

}
