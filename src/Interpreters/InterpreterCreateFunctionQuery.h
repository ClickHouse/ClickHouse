#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;

class InterpreterCreateLambdaFunctionQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateLambdaFunctionQuery(const ASTPtr & query_ptr_, ContextPtr context_, bool persist_function_)
        : WithContext(context_)
        , query_ptr(query_ptr_)
        , persist_function(persist_function_) {}

    BlockIO execute() override;

    void setInternal(bool internal_);

private:
    static void validateFunction(ASTPtr function, const String & name);
    static void validateFunctionRecursiveness(ASTPtr node, const String & function_to_create);

    ASTPtr query_ptr;
    bool persist_function;
};

class InterpreterCreateInterpFunctionQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateInterpFunctionQuery(const ASTPtr & query_ptr_, ContextPtr context_, bool persist_function_)
        : WithContext(context_)
        , query_ptr(query_ptr_)
        , persist_function(persist_function_) {}

    BlockIO execute() override;
    // TODO remove duplicate code

    void setInternal(bool internal_);

private:
    ASTPtr query_ptr;
    bool persist_function;
};

}
