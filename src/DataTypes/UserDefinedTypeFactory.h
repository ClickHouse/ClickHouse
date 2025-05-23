#pragma once

#include <unordered_map>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <Common/SharedMutex.h>
#include <Common/ThreadPool.h>
#include <Storages/IStorage.h>

// Forward declarations
namespace DB
{
class IParser;
class Context;
class Block;
class ParserDataType;
class ParserExpressionList;
using ContextPtr = std::shared_ptr<const Context>;
using ContextMutablePtr = std::shared_ptr<Context>;
using StoragePtr = std::shared_ptr<IStorage>;
}

namespace Poco
{
class Logger;
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
        String create_query_string;
    };

    static UserDefinedTypeFactory & instance();

    void registerType(
        ContextPtr context,
        const String & name,
        const ASTPtr & base_type_ast,
        ASTPtr type_parameters,
        const String & input_expression,
        const String & output_expression,
        const String & default_expression = "",
        const String & create_query_string = "");

    void removeType(ContextPtr context, const String & name, bool if_exists = false);

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

    // Методы для работы с системной таблицей
    void loadTypesFromSystemTable(ContextPtr context);
    void loadTypesFromSystemTableUnsafe(ContextPtr context);
    void ensureSystemTableExists(ContextPtr context);
    void createSystemTable(ContextPtr context);
    StoragePtr getSystemTable(ContextPtr context) const;
    
    // Методы для загрузки и сохранения данных
    void loadTypesFromStorage(ContextPtr context, StoragePtr storage);
    void processUDTBlock(
        const Block & block, 
        ParserDataType & data_type_parser, 
        ParserExpressionList & expression_list_parser,
        Poco::Logger * log);
    
    void storeTypeInSystemTable(ContextPtr context, const String & name, const TypeInfo & info);
    void removeTypeFromSystemTable(ContextPtr context, const String & name);
    
    // Вспомогательные методы
    String getNullableString(const ColumnPtr & column, size_t index) const;

    // Флаг, чтобы избежать повторной загрузки
    mutable bool types_loaded_from_db = false;

    mutable SharedMutex mutex;
    std::unordered_map<String, TypeInfo> types;
};

}
