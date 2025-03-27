#pragma once

#include <string>
#include <unordered_map>

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

struct DriverConfiguration
{
    String name;
    String format;
    String command;
};

struct UserDefinedDriverFunctionArgument
{
    DataTypePtr type;
    String name;
};

struct UserDefinedDriverFunctionConfiguration
{
    String name;
    std::vector<UserDefinedDriverFunctionArgument> arguments;
    DataTypePtr result_type;
    String driver_name;
    String body;
};

class UserDefinedDriverFunctionFactory
{
public:
    UserDefinedDriverFunctionFactory();

    static UserDefinedDriverFunctionFactory & instance();

    bool registerFunction(const ContextMutablePtr & context, const String & function_name, ASTPtr query, bool throw_if_exists, bool replace_if_exists);

    bool unregisterFunction(const ContextMutablePtr & context, const String & function_name, bool throw_if_not_exists);

    FunctionOverloadResolverPtr get(const String & function_name) const;

    FunctionOverloadResolverPtr tryGet(const String & function_name) const;

    bool has(const String & function_name) const;

    std::vector<String> getAllRegisteredNames() const;

    bool empty() const;

private:
    void checkCanBeRegistered(const ContextPtr & context, const String & function_name, const ASTPtr & query) const;
    void checkDriverExists(const ASTPtr & query) const;
    void registerDrivers();

    static void checkCanBeUnregistered(const ContextPtr & context, const String & function_name);

    std::unordered_map<String, DriverConfiguration> drivers;
    ContextPtr global_context = Context::getGlobalContextInstance();
};

}
