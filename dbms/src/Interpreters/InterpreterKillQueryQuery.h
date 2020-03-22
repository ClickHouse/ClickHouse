#pragma once

#include <Core/Block.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;
class AccessRightsElements;


class InterpreterKillQueryQuery : public IInterpreter
{
public:
    InterpreterKillQueryQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

private:
    AccessRightsElements getRequiredAccessForDDLOnCluster() const;
    Block getSelectResult(const String & columns, const String & table);

    ASTPtr query_ptr;
    Context & context;
};


}
