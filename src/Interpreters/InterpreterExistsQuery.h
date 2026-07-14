#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTQueryWithTableAndOutput;

/** Check that table exists. Return single row with single column "result" of type UInt8 and value 0 or 1.
  */
class InterpreterExistsQuery : public IInterpreter, WithContext
{
public:
    InterpreterExistsQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;

    static Block getSampleBlock();

private:
    ASTPtr query_ptr;

    QueryPipeline executeImpl();
    StorageID resolveExistsTarget(const ASTQueryWithTableAndOutput & query) const;
};


}
