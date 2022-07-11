#include <Interpreters/SubqueryForSet.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/PreparedSets.h>

namespace DB
{

SubqueryForSet::SubqueryForSet() = default;
SubqueryForSet::~SubqueryForSet() = default;
SubqueryForSet::SubqueryForSet(SubqueryForSet &&) = default;
SubqueryForSet & SubqueryForSet::operator= (SubqueryForSet &&) = default;

}
