#pragma once

#include <Interpreters/IInterpreter.h>

namespace DB
{

class ASTCreateDataTypeQuery;

class InterpreterCreateDataTypeQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateDataTypeQuery(const ASTPtr & query_ptr_, ContextPtr context_, bool is_internal_)
        : WithContext(context_)
        , query_ptr(query_ptr_)
        , is_internal(is_internal_) {}

    BlockIO execute() override;

    void setInternal(bool is_internal_);

private:
    ASTPtr query_ptr;
    bool is_internal;
};

}
