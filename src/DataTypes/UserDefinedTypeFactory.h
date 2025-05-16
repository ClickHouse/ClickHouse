#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <Common/ThreadPool.h>
#include <Common/SharedMutex.h>
#include <unordered_map>
#include <mutex>

// Forward declarations
namespace DB 
{
    class IParser;
    class Context;
    using ContextPtr = std::shared_ptr<const Context>;
}

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
        // Возможно, здесь стоит хранить и create_query, если он не будет только в БД
    };

    static UserDefinedTypeFactory & instance();

    // Метод для загрузки типов из БД
    void loadTypesFromSystemTable(ContextPtr context);

    void registerType(
        const String & name,
        const ASTPtr & base_type_ast,
        ASTPtr type_parameters,
        const String & input_expression,
        const String & output_expression,
        const String & default_expression = "");

    void removeType(const String & name);

    TypeInfo getTypeInfo(const String & name, ContextPtr context) const;

    bool isTypeRegistered(const String & name, ContextPtr context) const;

    std::vector<String> getAllTypeNames(ContextPtr context) const;

    // Вспомогательные функции для сериализации/десериализации AST
    static String astToString(const ASTPtr & ast);
    static ASTPtr stringToAst(const String & str, IParser & parser);

private:
    UserDefinedTypeFactory() = default;

    // Приватный метод для ленивой загрузки
    void ensureTypesLoaded(ContextPtr context) const;

    // Флаг, чтобы избежать повторной загрузки
    mutable bool types_loaded_from_db = false;

    mutable std::recursive_mutex mutex;
    std::unordered_map<String, TypeInfo> types;

    // static std::unique_ptr<UserDefinedTypeFactory> the_instance; // a-la Myers singleton, the_instance не нужен
};

}
