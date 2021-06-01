#include <Interpreters/InterpreterDeleteQuery.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

namespace DB
{

InterpreterDeleteQuery::InterpreterDeleteQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{ }


BlockIO InterpreterDeleteQuery::execute()
{
    return {};
}
}
