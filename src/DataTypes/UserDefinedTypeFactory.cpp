#include <DataTypes/UserDefinedTypeFactory.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Parsers/IAST.h>
#include <Parsers/IParser.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/ExpressionListParsers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/QueryFlags.h>
#include <Interpreters/DatabaseCatalog.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/quoteString.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <Databases/IDatabase.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_TEXT;
    extern const int BAD_ARGUMENTS;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_DATABASE;
}

UserDefinedTypeFactory & UserDefinedTypeFactory::instance()
{
    static UserDefinedTypeFactory udt_factory;
    return udt_factory;
}

void UserDefinedTypeFactory::ensureTypesLoaded(ContextPtr context) const
{
    if (types_loaded_from_db)
        return;

    std::unique_lock lock(mutex);
    if (!types_loaded_from_db)
    {
        const_cast<UserDefinedTypeFactory*>(this)->loadTypesFromSystemTableUnsafe(context);
    }
}

void UserDefinedTypeFactory::registerType(
    ContextPtr context,
    const String & name,
    const ASTPtr & base_type_ast,
    ASTPtr type_parameters,
    const String & input_expression,
    const String & output_expression,
    const String & default_expression,
    const String & create_query_string)
{
    TypeInfo info;
    info.base_type_ast = base_type_ast;
    info.type_parameters = type_parameters;
    info.input_expression = input_expression;
    info.output_expression = output_expression;
    info.default_expression = default_expression;
    info.create_query_string = create_query_string;

    bool should_store_in_system_table = false;

    {
        std::unique_lock lock(mutex);
        
        if (types.contains(name))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type with name {} already registered", name);

        types[name] = info;
        should_store_in_system_table = (context && types_loaded_from_db);
    }

    if (should_store_in_system_table)
    {
        try
        {
            storeTypeInSystemTable(context, name, info);
        }
        catch (const DB::Exception & e)
        {
            LOG_ERROR(&Poco::Logger::get("UserDefinedTypeFactory"), 
                     "Failed to store type '{}' in system table. Rolling back in-memory registration. Error: {}", 
                     name, e.what());
            
            {
                std::unique_lock rollback_lock(mutex);
                types.erase(name);
            }
            throw;
        }
    }
}

void UserDefinedTypeFactory::removeType(ContextPtr context, const String & name, bool if_exists)
{
    TypeInfo saved_info;
    bool type_existed = false;
    bool should_remove_from_system_table = false;

    {
        std::unique_lock lock(mutex);
        
        auto it = types.find(name);
        if (it == types.end())
        {
            if (if_exists)
                return;
            else
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown type {}", name);
        }
        
        saved_info = it->second;
        type_existed = true;
        
        types.erase(it);
        should_remove_from_system_table = (context && types_loaded_from_db);
    }

    if (should_remove_from_system_table && type_existed)
    {
        try
        {
            removeTypeFromSystemTable(context, name);
        }
        catch (const DB::Exception & e)
        {
            LOG_ERROR(&Poco::Logger::get("UserDefinedTypeFactory"), 
                     "Failed to remove type '{}' from system table. Rolling back in-memory removal. Error: {}", 
                     name, e.what());
            
            {
                std::unique_lock rollback_lock(mutex);
                types[name] = saved_info;
            }
            throw;
        }
    }
}

UserDefinedTypeFactory::TypeInfo UserDefinedTypeFactory::getTypeInfo(const String & name, ContextPtr context) const
{
    ensureTypesLoaded(context);
    std::shared_lock lock(mutex);
    
    auto it = types.find(name);
    if (it == types.end())
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown type {}", name);

    return it->second;
}

bool UserDefinedTypeFactory::isTypeRegistered(const String & name, ContextPtr context) const
{
    ensureTypesLoaded(context);
    std::shared_lock lock(mutex);
    return types.contains(name);
}

std::vector<String> UserDefinedTypeFactory::getAllTypeNames(ContextPtr context) const
{
    ensureTypesLoaded(context);
    std::shared_lock lock(mutex);
    
    std::vector<String> result;
    result.reserve(types.size());
    
    for (const auto & [typeName, _] : types)
        result.push_back(typeName);
        
    return result;
}

void UserDefinedTypeFactory::loadTypesFromSystemTable(ContextPtr context_ptr)
{
    std::unique_lock lock(mutex);
    if (types_loaded_from_db)
        return;

    loadTypesFromSystemTableUnsafe(context_ptr);
    types_loaded_from_db = true;
}

void UserDefinedTypeFactory::loadTypesFromSystemTableUnsafe(ContextPtr context_ptr)
{
    if (types_loaded_from_db)
        return;

    auto * log = &Poco::Logger::get("UserDefinedTypeFactory");

    if (!context_ptr)
    {
        LOG_ERROR(log, "Context pointer is null in loadTypesFromSystemTableUnsafe. Cannot load UDTs.");
        types_loaded_from_db = true;
        return;
    }

    try
    {
        auto storage = getSystemTable(context_ptr);
        if (!storage)
        {
            types_loaded_from_db = true;
            return;
        }

        loadTypesFromStorage(context_ptr, storage);
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(log, "Failed to load user-defined types from system table. Error: {}. Code: {}.", 
                    e.what(), e.code());
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to load user-defined types from system table due to an unknown exception.");
    }
    
    types_loaded_from_db = true;
}

void UserDefinedTypeFactory::ensureSystemTableExists(ContextPtr context_ptr)
{
    auto * log = &Poco::Logger::get("UserDefinedTypeFactory");
    
    try
    {
        auto & database_catalog = DatabaseCatalog::instance();
        
        if (!database_catalog.isDatabaseExist("udt"))
        {
            ContextMutablePtr mutable_context = Context::createCopy(context_ptr);
            String create_database_query = "CREATE DATABASE IF NOT EXISTS udt";
            
            auto res = executeQuery(create_database_query, mutable_context, QueryFlags{.internal = true});
            if (res.second.pipeline.initialized())
            {
                QueryPipeline pipeline = std::move(res.second.pipeline);
                CompletedPipelineExecutor executor(pipeline);
                executor.execute();
            }
        }

        StorageID table_id("udt", "user_defined_types");
        if (!database_catalog.isTableExist(table_id, context_ptr))
        {
            createSystemTable(context_ptr);
        }
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to ensure system table exists. Error: {}. Code: {}.", e.what(), e.code());
        throw;
    }
}

void UserDefinedTypeFactory::createSystemTable(ContextPtr context_ptr)
{
    ContextMutablePtr mutable_context = Context::createCopy(context_ptr);
    
    String create_table_query = R"(
        CREATE TABLE IF NOT EXISTS udt.user_defined_types
        (
            name String,
            base_type_ast_string String,
            type_parameters_ast_string Nullable(String),
            input_expression Nullable(String),
            output_expression Nullable(String),
            default_expression Nullable(String),
            create_query_string String
        )
        ENGINE = MergeTree()
        ORDER BY name
        PRIMARY KEY name
        SETTINGS index_granularity = 8192
    )";

    auto res = executeQuery(create_table_query, mutable_context, QueryFlags{.internal = true});
    if (res.second.pipeline.initialized())
    {
        QueryPipeline pipeline = std::move(res.second.pipeline);
        CompletedPipelineExecutor executor(pipeline);
        executor.execute();
    }
}

StoragePtr UserDefinedTypeFactory::getSystemTable(ContextPtr context_ptr) const
{
    try
    {
        StorageID table_id("udt", "user_defined_types");
        return DatabaseCatalog::instance().getTable(table_id, context_ptr);
    }
    catch (...)
    {
        return nullptr;
    }
}

void UserDefinedTypeFactory::loadTypesFromStorage(ContextPtr context_ptr, StoragePtr /*storage*/)
{
    auto * log = &Poco::Logger::get("UserDefinedTypeFactory");
    
    try
    {
        ContextMutablePtr mutable_context = Context::createCopy(context_ptr);
        String query = "SELECT name, base_type_ast_string, type_parameters_ast_string, input_expression, output_expression, default_expression, create_query_string FROM udt.user_defined_types";
        
        auto res = executeQuery(query, mutable_context, QueryFlags{.internal = true});
        if (!res.second.pipeline.initialized())
        {
            LOG_WARNING(log, "Pipeline for loading UDTs not initialized.");
            return;
        }

        QueryPipeline pipeline = std::move(res.second.pipeline);
        PullingPipelineExecutor executor(pipeline);
        Block block;
        ParserDataType data_type_parser;
        ParserExpressionList expression_list_parser(false);

        while (executor.pull(block))
        {
            if (!block || block.rows() == 0) 
                continue;

            processUDTBlock(block, data_type_parser, expression_list_parser, log);
        }
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to load types from storage. Error: {}. Code: {}.", e.what(), e.code());
        throw;
    }
}

void UserDefinedTypeFactory::processUDTBlock(
    const Block & block, 
    ParserDataType & data_type_parser, 
    ParserExpressionList & expression_list_parser,
    Poco::Logger * log)
{
    const auto & columns = block.getColumnsWithTypeAndName();
    if (columns.size() != 7)
    {
        LOG_ERROR(log, "Unexpected number of columns in UDT table: {}", columns.size());
        return;
    }

    const auto & name_col = columns[0].column;
    const auto & base_ast_col = columns[1].column;
    const auto & params_ast_col = columns[2].column;
    const auto & input_expr_col = columns[3].column;
    const auto & output_expr_col = columns[4].column;
    const auto & default_expr_col = columns[5].column;
    const auto & create_query_col = columns[6].column;

    for (size_t i = 0; i < name_col->size(); ++i)
    {
        try
        {
            String type_name = name_col->getDataAt(i).toString();
            String base_ast_str = base_ast_col->getDataAt(i).toString();
                    
            String params_ast_str = getNullableString(params_ast_col, i);
            String input_expr_str = getNullableString(input_expr_col, i);
            String output_expr_str = getNullableString(output_expr_col, i);
            String default_expr_str = getNullableString(default_expr_col, i);
            String create_query_str = create_query_col->getDataAt(i).toString();

            ASTPtr base_type_ast = stringToAst(base_ast_str, data_type_parser);
            if (!base_type_ast)
            {
                LOG_WARNING(log, "Failed to parse base_type_ast for type '{}'. Skipping.", type_name);
                continue;
            }

            ASTPtr params_ast = nullptr;
            if (!params_ast_str.empty())
            {
                params_ast = stringToAst(params_ast_str, expression_list_parser);
                if (!params_ast)
                {
                    LOG_WARNING(log, "Failed to parse type_parameters_ast for type '{}'. Skipping parameters.", type_name);
                }
            }
            
            TypeInfo info;
            info.base_type_ast = base_type_ast;
            info.type_parameters = params_ast;
            info.input_expression = input_expr_str;
            info.output_expression = output_expr_str;
            info.default_expression = default_expr_str;
            info.create_query_string = create_query_str;
            types[type_name] = info;
        }
        catch (const Exception & e)
        {
            LOG_ERROR(log, "Failed to process UDT row {}. Error: {}. Code: {}.", i, e.what(), e.code());
        }
    }
}

String UserDefinedTypeFactory::getNullableString(const ColumnPtr & column, size_t index) const
{
    if (const ColumnNullable * col_nullable = checkAndGetColumn<ColumnNullable>(column.get()))
    {
        if (!col_nullable->isNullAt(index))
            return col_nullable->getNestedColumn().getDataAt(index).toString();
    }
    return "";
}

void UserDefinedTypeFactory::storeTypeInSystemTable(ContextPtr context, const String & name, const TypeInfo & info)
{
    auto * log = &Poco::Logger::get("UserDefinedTypeFactory");
    
    try
    {
        ensureSystemTableExists(context);
        
        ContextMutablePtr mutable_context = Context::createCopy(context);

        String query = fmt::format(
            "INSERT INTO udt.user_defined_types (name, base_type_ast_string, type_parameters_ast_string, input_expression, output_expression, default_expression, create_query_string) VALUES ({}, {}, {}, {}, {}, {}, {})",
            quoteString(name),
            quoteString(astToString(info.base_type_ast)),
            (info.type_parameters ? quoteString(astToString(info.type_parameters)) : "NULL"),
            (!info.input_expression.empty() ? quoteString(info.input_expression) : "NULL"),
            (!info.output_expression.empty() ? quoteString(info.output_expression) : "NULL"),
            (!info.default_expression.empty() ? quoteString(info.default_expression) : "NULL"),
            quoteString(info.create_query_string)
        );
        
        auto res = executeQuery(query, mutable_context, QueryFlags{.internal = true});
        if (res.second.pipeline.initialized())
        {
            QueryPipeline pipeline = std::move(res.second.pipeline);
            CompletedPipelineExecutor executor(pipeline);
            executor.execute();
        }
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(log, "Failed to insert user-defined type '{}' into system table. Error: {}. Code: {}.", name, e.what(), e.code());
    }
}

void UserDefinedTypeFactory::removeTypeFromSystemTable(ContextPtr context, const String & name)
{
    auto * log = &Poco::Logger::get("UserDefinedTypeFactory");
    
    try
    {
        ensureSystemTableExists(context);
        
        ContextMutablePtr mutable_context = Context::createCopy(context);
        String query = fmt::format("DELETE FROM udt.user_defined_types WHERE name = {}", quoteString(name));
        
        auto res = executeQuery(query, mutable_context, QueryFlags{.internal = true});
        if (res.second.pipeline.initialized())
        {
            QueryPipeline pipeline = std::move(res.second.pipeline);
            CompletedPipelineExecutor executor(pipeline);
            executor.execute();
        }
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(log, "Failed to delete user-defined type '{}' from system table. Error: {}. Code: {}.", name, e.what(), e.code());
    }
}

String UserDefinedTypeFactory::astToString(const ASTPtr & ast)
{
    if (!ast)
        return "";

    WriteBufferFromOwnString ostr_buf;
    IAST::FormatSettings settings(true /*one_line*/);
    settings.show_secrets = false;

    ast->format(ostr_buf, settings);
    return ostr_buf.str();
}

ASTPtr UserDefinedTypeFactory::stringToAst(const String & str, IParser & parser)
{
    if (str.empty())
        return nullptr;

    try
    {
        return parseQuery(parser, str, "UserDefinedTypeFactory AST deserialization", 
                        0, DBMS_DEFAULT_MAX_PARSER_DEPTH, 
                        DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const DB::Exception & e)
    {
        LOG_WARNING(&Poco::Logger::get("UserDefinedTypeFactory"), 
                    "Failed to deserialize AST. Error: {}. Input string: '{}'", e.what(), str);
        return nullptr;
    }
}

}
