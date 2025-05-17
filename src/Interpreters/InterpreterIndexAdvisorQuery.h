#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Utils/IndexAdvisor/IndexAdvisor.h>

namespace DB
{

class ASTIndexAdvisorQuery;

class InterpreterIndexAdvisorQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterIndexAdvisorQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    bool supportsTransactions() const override { return false; }

private:
    ASTPtr query_ptr;
};

} 
