#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST.h>


namespace DB
{

class Context;


class InterpreterKillQueryQuery : public IInterpreter
{
public:
    InterpreterKillQueryQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    Block getSelectResult(const String & columns, const String & table);

    ASTPtr query_ptr;
    Context & context;
};


}
