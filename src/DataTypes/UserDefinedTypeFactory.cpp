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
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>

#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_TEXT;
    extern const int BAD_ARGUMENTS;
}

UserDefinedTypeFactory & UserDefinedTypeFactory::instance()
{
    static UserDefinedTypeFactory udt_factory;
    return udt_factory;
}

void UserDefinedTypeFactory::ensureTypesLoaded(ContextPtr context) const
{
    std::lock_guard<std::recursive_mutex> lock(mutex);
    if (!types_loaded_from_db)
    {
        const_cast<UserDefinedTypeFactory*>(this)->loadTypesFromSystemTable(context);
    }
}

void UserDefinedTypeFactory::registerType(
    const String & name,
    const ASTPtr & base_type_ast,
    ASTPtr type_parameters,
    const String & input_expression,
    const String & output_expression,
    const String & default_expression)
{
    std::lock_guard<std::recursive_mutex> lock(mutex);
    
    if (types.contains(name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Type with name {} already registered", name);

    TypeInfo info;
    info.base_type_ast = base_type_ast;
    info.type_parameters = type_parameters;
    info.input_expression = input_expression;
    info.output_expression = output_expression;
    info.default_expression = default_expression;

    types[name] = info;
    
    LOG_INFO(&Poco::Logger::get("UserDefinedTypeFactory"), "Registered user-defined type '{}' with base type AST: {}", 
        name, UserDefinedTypeFactory::astToString(base_type_ast));
}

void UserDefinedTypeFactory::removeType(const String & name)
{
    std::lock_guard<std::recursive_mutex> lock(mutex);
    
    if (!types.contains(name))
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown type {}", name);

    types.erase(name);
    
    LOG_INFO(&Poco::Logger::get("UserDefinedTypeFactory"), "Removed user-defined type '{}'", name);
}

UserDefinedTypeFactory::TypeInfo UserDefinedTypeFactory::getTypeInfo(const String & name, ContextPtr context) const
{
    ensureTypesLoaded(context);
    std::lock_guard<std::recursive_mutex> lock(mutex);
    
    auto it = types.find(name);
    if (it == types.end())
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown type {}", name);

    return it->second;
}

bool UserDefinedTypeFactory::isTypeRegistered(const String & name, ContextPtr context) const
{
    ensureTypesLoaded(context);
    std::lock_guard<std::recursive_mutex> lock(mutex);
    return types.contains(name);
}

std::vector<String> UserDefinedTypeFactory::getAllTypeNames(ContextPtr context) const
{
    ensureTypesLoaded(context);
    std::lock_guard<std::recursive_mutex> lock(mutex);
    
    std::vector<String> result;
    result.reserve(types.size());
    
    for (const auto & [typeName, _] : types)
        result.push_back(typeName);
        
    return result;
}

void UserDefinedTypeFactory::loadTypesFromSystemTable(ContextPtr context_ptr)
{
    std::lock_guard<std::recursive_mutex> lock(mutex);
    if (types_loaded_from_db)
        return;

    LOG_DEBUG(&Poco::Logger::get("UserDefinedTypeFactory"), "Loading user-defined types from system table.");

    ContextMutablePtr mutable_context = Context::createCopy(context_ptr);

    try
    {
        String query = "SELECT name, base_type_ast_string, type_parameters_ast_string, input_expression, output_expression, default_expression FROM system.user_defined_types";
        
        BlockIO current_block_io = executeQuery(
            query, 
            mutable_context, 
            QueryFlags{ .internal = true }, 
            QueryProcessingStage::Complete
        ).second;
        
        if (!current_block_io.pipeline.initialized())
        {
            LOG_WARNING(&Poco::Logger::get("UserDefinedTypeFactory"), "Failed to initialize pipeline for loading UDTs. system.user_defined_types might be empty or query failed.");
            types_loaded_from_db = true;
            return;
        }

        QueryPipeline pipeline = std::move(current_block_io.pipeline);
        Block block;
        ParserDataType data_type_parser;
        ParserExpressionList expression_list_parser(false /*no_aliases*/);

        PullingPipelineExecutor executor(pipeline);
        while (executor.pull(block))
        {
            if (!block) continue;

            const auto & name_col_ptr = block.getByName("name").column;
            const auto & base_ast_col_ptr = block.getByName("base_type_ast_string").column;
            const auto & params_ast_col_nullable_ptr = block.getByName("type_parameters_ast_string").column;
            const auto & input_expr_col_nullable_ptr = block.getByName("input_expression").column;
            const auto & output_expr_col_nullable_ptr = block.getByName("output_expression").column;
            const auto & default_expr_col_nullable_ptr = block.getByName("default_expression").column;

            for (size_t i = 0; i < name_col_ptr->size(); ++i)
            {
                String type_name = name_col_ptr->getDataAt(i).toString();
                String base_ast_str = base_ast_col_ptr->getDataAt(i).toString();
                
                String params_ast_str;
                if (const ColumnNullable * col_nullable = checkAndGetColumn<ColumnNullable>(params_ast_col_nullable_ptr.get()))
                {
                    if (!col_nullable->isNullAt(i))
                        params_ast_str = col_nullable->getNestedColumn().getDataAt(i).toString();
                }
                else
                {
                     LOG_ERROR(&Poco::Logger::get("UserDefinedTypeFactory"), "Column type_parameters_ast_string is not Nullable as expected for type '{}'.", type_name);
                     continue;
                }

                String input_expr_str;
                String output_expr_str;
                String default_expr_str;
                auto get_string_from_nullable = [&](const IColumn * col_nullable_generic_ptr, const String & col_name) -> String {
                    if (const ColumnNullable * col_nullable_typed = checkAndGetColumn<ColumnNullable>(col_nullable_generic_ptr)) {
                        if (!col_nullable_typed->isNullAt(i))
                            return col_nullable_typed->getNestedColumn().getDataAt(i).toString();
                    } else if (col_nullable_generic_ptr) {
                        LOG_ERROR(&Poco::Logger::get("UserDefinedTypeFactory"), "Column '{}' is not Nullable as expected for type '{}'.", col_name, type_name);
                    }
                    return ""; 
                };

                input_expr_str = get_string_from_nullable(input_expr_col_nullable_ptr.get(), "input_expression");
                output_expr_str = get_string_from_nullable(output_expr_col_nullable_ptr.get(), "output_expression");
                default_expr_str = get_string_from_nullable(default_expr_col_nullable_ptr.get(), "default_expression");

                ASTPtr base_type_ast = stringToAst(base_ast_str, data_type_parser);
                if (!base_type_ast)
                {
                    LOG_WARNING(&Poco::Logger::get("UserDefinedTypeFactory"), "Failed to parse base_type_ast for type '{}'. Skipping.", type_name);
                    continue;
                }

                ASTPtr params_ast = nullptr;
                if (!params_ast_str.empty())
                {
                    params_ast = stringToAst(params_ast_str, expression_list_parser);
                    if (!params_ast)
                    {
                        LOG_WARNING(&Poco::Logger::get("UserDefinedTypeFactory"), "Failed to parse type_parameters_ast for type '{}'. Skipping parameters.", type_name);
                    }
                }
                
                TypeInfo info;
                info.base_type_ast = base_type_ast;
                info.type_parameters = params_ast;
                info.input_expression = input_expr_str;
                info.output_expression = output_expr_str;
                info.default_expression = default_expr_str;
                types[type_name] = info;

                LOG_DEBUG(&Poco::Logger::get("UserDefinedTypeFactory"), "Successfully loaded type '{}' from system table.", type_name);
            }
            block.clear();
        }
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(&Poco::Logger::get("UserDefinedTypeFactory"), "Failed to load user-defined types from system table. Error: {}. Code: {}.", e.what(), e.code());
        throw;
    }
    catch (...)
    {
        LOG_ERROR(&Poco::Logger::get("UserDefinedTypeFactory"), "Failed to load user-defined types from system table due to an unknown exception.");
        throw;
    }
    
    types_loaded_from_db = true;
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
