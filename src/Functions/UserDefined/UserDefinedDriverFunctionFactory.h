#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Functions/UserDefined/UserDefinedDriver.h>
#include <Functions/UserDefined/UserDefinedExecutableFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateDriverFunctionQuery.h>


namespace DB
{

class UserDefinedDriverFunctionFactory
{
public:
    static UserDefinedDriverFunctionFactory & instance();

    bool registerFunction(const ContextMutablePtr & context, const String & function_name, ASTPtr query, bool throw_if_exists, bool replace_if_exists);

    bool unregisterFunction(const ContextMutablePtr & context, const String & function_name, bool throw_if_not_exists);

    FunctionOverloadResolverPtr get(const String & function_name) const;

    FunctionOverloadResolverPtr tryGet(const String & function_name) const;

    bool has(const String & function_name) const;

    std::vector<String> getAllRegisteredNames() const;

    bool empty() const;

private:
    void checkCanBeRegistered(const String & function_name, const ASTPtr & query) const;
    void checkCanBeUnregistered(const String & function_name);
    void checkDriverExists(const ASTPtr & query) const;
    UserDefinedExecutableFunctionPtr createUserDefinedFunction(const ASTCreateDriverFunctionQuery & query, const UserDefinedDriverPtr & driver) const;

    ContextPtr global_context = Context::getGlobalContextInstance();
};

}
