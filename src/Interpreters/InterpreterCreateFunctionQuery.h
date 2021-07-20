#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTCreateFunctionQuery.h>

namespace DB
{

class ASTCreateFunctionQuery;
class Context;

class InterpreterCreateFunctionQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateFunctionQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    void setInternal(bool internal_);

private:
    static void validateFunction(ASTPtr function, const String & name);
    static void getIdentifiers(ASTPtr node, std::set<String> & identifiers);
    static void validateFunctionRecursiveness(ASTPtr node, const String & function_to_create);

private:
    ASTPtr query_ptr;

    /// Is this an internal query - not from the user.
    bool internal = false;
};

}
