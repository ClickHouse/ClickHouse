#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/** Return names, types and other information about columns in specified table.
  */
class InterpreterDescribeQuery : public IInterpreter, WithContext
{
public:
    InterpreterDescribeQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static Block getSampleBlock(bool include_subcolumns);

private:
    ASTPtr query_ptr;
};


}
