#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTAnalyzeQuery.h>


namespace DB
{
class InterpreterAnalyzeQuery : public IInterpreter
{
public:
    InterpreterAnalyzeQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    BlockIO executeAnalyzeTable();

    ASTPtr query_ptr;
    ContextMutablePtr context;
};
}
