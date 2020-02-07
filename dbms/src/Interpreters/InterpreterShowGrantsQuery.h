#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTShowGrantsQuery;


class InterpreterShowGrantsQuery : public IInterpreter
{
public:
    InterpreterShowGrantsQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    BlockInputStreamPtr executeImpl();
    ASTs getGrantQueries(const ASTShowGrantsQuery & show_query) const;

    ASTPtr query_ptr;
    Context & context;
};
}
