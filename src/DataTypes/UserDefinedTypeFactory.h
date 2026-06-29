#pragma once

#include <unordered_map>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <Common/SharedMutex.h>
#include <Common/ThreadPool.h>
#include <Storages/IStorage.h>


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
    /// Describes a single user-defined type. For example, for
    ///     CREATE TYPE CustomType(T, U) AS Tuple(T, Array(U))
    /// the fields are:
    struct TypeInfo
    {
        /// The base type the user-defined type expands to: `Tuple(T, Array(U))`.
        /// It may reference the formal parameters and other user-defined types.
        ASTPtr base_type_ast;
        /// The formal parameters of a parameterized type as an expression list: `T, U`.
        /// `nullptr` for a non-parameterized type.
        ASTPtr type_parameters;
        /// The `INPUT` / `OUTPUT` / `DEFAULT` clauses, if specified. Currently they are
        /// only recorded here (and exposed in `system.user_defined_types`); they do not
        /// yet affect parsing, formatting, or the column default.
        String input_expression;
        String output_expression;
        String default_expression;
        /// The original `CREATE TYPE` query text, as shown by `SHOW TYPE`.
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

    static String astToString(const ASTPtr & ast);
    static ASTPtr stringToAst(const String & str, IParser & parser);

private:
    UserDefinedTypeFactory() = default;

    void ensureTypesLoaded(ContextPtr context) const;

    void loadTypesFromSystemTable(ContextPtr context);
    void loadTypesFromSystemTableUnsafe(ContextPtr context);
    void ensureSystemTableExists(ContextPtr context);
    void createSystemTable(ContextPtr context);
    StoragePtr getSystemTable(ContextPtr context) const;
    
    void loadTypesFromStorage(ContextPtr context, StoragePtr storage);
    void processUDTBlock(
        const Block & block, 
        ParserDataType & data_type_parser, 
        ParserExpressionList & expression_list_parser,
        Poco::Logger * log);
    
    void storeTypeInSystemTable(ContextPtr context, const String & name, const TypeInfo & info);
    void removeTypeFromSystemTable(ContextPtr context, const String & name);
    
    String getNullableString(const ColumnPtr & column, size_t index) const;

    mutable bool types_loaded_from_db = false;

    mutable SharedMutex mutex;
    std::unordered_map<String, TypeInfo> types;
};

}
