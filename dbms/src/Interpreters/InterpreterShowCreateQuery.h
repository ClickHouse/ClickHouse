#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


/** Return single row with single column "statement" of type String with text of query to CREATE specified table.
  */
class InterpreterShowCreateQuery : public IInterpreter
{
public:
    InterpreterShowCreateQuery(const ASTPtr & query_ptr_, const Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    static Block getSampleBlock();

private:
    ASTPtr query_ptr;
    const Context & context;

    BlockInputStreamPtr executeImpl();
};


}
