#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST.h>

namespace DB
{

class Context;
class Cluster;

class InterpreterCheckQuery : public IInterpreter
{
public:
    /// Set remote tables implicitly, via existing Distributed table
    InterpreterCheckQuery(const ASTPtr & query_ptr_with_local_table_, const Context & context_);

    struct RemoteTablesInfo
    {
        std::shared_ptr<Cluster> cluster;
        String remote_database;
        String remote_table;
    };

    /// Set remote tables explicitly
    InterpreterCheckQuery(RemoteTablesInfo remote_tables_, const Context & context_);

    BlockIO execute() override;

private:
    Block getSampleBlock() const;

private:
    ASTPtr query_ptr;
    RemoteTablesInfo remote_tables;

    const Context & context;
    Block result;
};

}
