#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/IInterpreter.h>

namespace DB
{

namespace MySQLInterpreter
{

/**
  */
class InterpreterMySQLCreateQuery : public IInterpreter
{
public:
    InterpreterMySQLCreateQuery(const ASTPtr & query_ptr_, Context & context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;

    ASTPtr getRewrittenQuery();
};

}

}
