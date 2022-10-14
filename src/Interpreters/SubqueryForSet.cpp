#include <Interpreters/SubqueryForSet.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/PreparedSets.h>

namespace DB
{

SubqueryForSet::SubqueryForSet() = default;
SubqueryForSet::~SubqueryForSet() = default;
SubqueryForSet::SubqueryForSet(SubqueryForSet &&) noexcept = default;
SubqueryForSet & SubqueryForSet::operator= (SubqueryForSet &&) noexcept = default;

}
