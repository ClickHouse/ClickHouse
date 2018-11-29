#pragma once
#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;
class IAST;
class ASTSystemQuery;
class IStorage;
using ASTPtr = std::shared_ptr<IAST>;
using StoragePtr = std::shared_ptr<IStorage>;


class InterpreterSystemQuery : public IInterpreter
{
public:
    InterpreterSystemQuery(const ASTPtr & query_ptr_, Context & context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;
    Poco::Logger * log = nullptr;

    /// Tries to get a replicated table and restart it
    /// Returns pointer to a newly created table if the restart was successful
    StoragePtr tryRestartReplica(const String & database_name, const String & table_name, Context & context);

    void restartReplicas(Context & system_context);
    void syncReplica(ASTSystemQuery & query);
};


}
