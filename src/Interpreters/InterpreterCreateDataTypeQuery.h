#pragma once

#include <Interpreters/IInterpreter.h>

namespace DB
{

class ASTCreateDataTypeQuery;

class InterpreterCreateDataTypeQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateDataTypeQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    void setInternal(bool internal_);

private:
    ASTPtr query_ptr;

    /// Is this an internal query - not from the user.
    bool internal = false;
};

}
