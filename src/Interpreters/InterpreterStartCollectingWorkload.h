#pragma once
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Core/Field.h>

namespace DB
{
class InterpreterStartCollectingWorkload : public IInterpreter, WithMutableContext
{
public:
    InterpreterStartCollectingWorkload(const ASTPtr &, ContextMutablePtr context_)
    : WithMutableContext(context_){}

    BlockIO execute() override
    {
        return executeQuery("SET collect_workload = 1", getContext(), QueryFlags{ .internal = true }).second;
    }
};

}
