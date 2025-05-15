#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <Common/ThreadPool.h>
#include <Common/SharedMutex.h>
#include <unordered_map>
#include <mutex>

namespace DB
{

class UserDefinedTypeFactory final : private boost::noncopyable
{
public:
    struct TypeInfo
    {
        ASTPtr base_type_ast;
        ASTPtr type_parameters;
        String input_expression;
        String output_expression;
        String default_expression;
    };

    static UserDefinedTypeFactory & instance();

    void registerType(
        const String & name,
        const ASTPtr & base_type_ast,
        ASTPtr type_parameters,
        const String & input_expression,
        const String & output_expression,
        const String & default_expression = "");

    void removeType(const String & name);

    TypeInfo getTypeInfo(const String & name) const;

    bool isTypeRegistered(const String & name) const;

    std::vector<String> getAllTypeNames() const;

private:
    UserDefinedTypeFactory() = default;

    mutable std::mutex mutex;
    std::unordered_map<String, TypeInfo> types;

    static std::unique_ptr<UserDefinedTypeFactory> the_instance;
};

}
