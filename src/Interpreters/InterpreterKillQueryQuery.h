#pragma once

#include <Core/Block.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class AccessRightsElements;

class InterpreterKillQueryQuery final : public IInterpreter, WithContext
{
public:
    InterpreterKillQueryQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;

private:
    AccessRightsElements getRequiredAccessForDDLOnCluster() const;
    Block getSelectResult(const String & columns, const String & table);

    ASTPtr query_ptr;
};

}
