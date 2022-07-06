#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class InterpreterDescribeCacheQuery : public IInterpreter, WithContext
{
public:
    InterpreterDescribeCacheQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};


}
