#pragma once
#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;
class IAST;
class ASTSystemQuery;
using ASTPtr = std::shared_ptr<IAST>;


class InterpreterSystemQuery : public IInterpreter
{
public:
    InterpreterSystemQuery(const ASTPtr & query_ptr_, Context & context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;
    Poco::Logger * log = nullptr;

    void restartReplicas();
    void syncReplica(ASTSystemQuery & query);
};


}
