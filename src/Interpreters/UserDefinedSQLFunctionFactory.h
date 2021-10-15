#pragma once

#include <unordered_map>
#include <mutex>

#include <Common/NamePrompter.h>

#include <Parsers/ASTCreateFunctionQuery.h>

namespace DB
{

class UserDefinedSQLFunctionFactory : public IHints<1, UserDefinedSQLFunctionFactory>
{
public:
    static UserDefinedSQLFunctionFactory & instance();

    void registerFunction(const String & function_name, ASTPtr create_function_query);

    void unregisterFunction(const String & function_name);

    ASTPtr get(const String & function_name) const;

    ASTPtr tryGet(const String & function_name) const;

    std::vector<String> getAllRegisteredNames() const override;

private:
    std::unordered_map<String, ASTPtr> function_name_to_create_query;
    mutable std::mutex mutex;
};

}
