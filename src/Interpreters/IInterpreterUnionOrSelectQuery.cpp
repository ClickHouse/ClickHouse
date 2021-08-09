#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <Interpreters/QueryLog.h>

namespace DB
{

void IInterpreterUnionOrSelectQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const
{
    elem.query_kind = "Select";
}

}
