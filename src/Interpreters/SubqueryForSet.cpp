#include <Interpreters/SubqueryForSet.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/PreparedSets.h>

namespace DB
{

SubqueryForSet::SubqueryForSet() = default;
SubqueryForSet::~SubqueryForSet() = default;
SubqueryForSet::SubqueryForSet(SubqueryForSet &&) noexcept = default;
SubqueryForSet & SubqueryForSet::operator= (SubqueryForSet &&) noexcept = default;

SubqueryForSet::SubqueryForSet(const SubqueryForSet & rhs) noexcept
{
    /*
     * Sometimes, we must pass SubqueryFroSet to child interpreters to reuse sets.
     * But we can't copy the source because it is a unique pointer to QueryPlan.
     * So, copy sets so that they can be reused.
     * Parent SubqueryForSet will execute the query plan, and it works because we need to execute it just once.
     */
    this->set = rhs.set;
    this->table = rhs.table;
}

}
