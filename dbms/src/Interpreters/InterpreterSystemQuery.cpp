#include "InterpreterSystemQuery.h"

namespace DB
{

InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

BlockIO InterpreterSystemQuery::execute()
{
    return BlockIO();
}

}