#include <Interpreters/InterpreterCreateTypeQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/IAST.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/UserDefinedTypeFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/QueryFlags.h>
#include <Core/QueryProcessingStage.h>
#include <IO/WriteBufferFromString.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_ALREADY_EXISTS;
    extern const int UNKNOWN_TYPE;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int BAD_ARGUMENTS; // For invalid type parameter usage
}

namespace // Anonymous namespace for helper functions
{

void validateBaseTypeRecursive(
    const ASTPtr & ast_node,
    const String & udt_name,
    const std::unordered_set<String> & formal_param_names,
    const DataTypeFactory & factory_instance,
    ContextPtr validation_context,
    const std::unordered_set<String> & known_families)
{
    if (!ast_node)
        return;

    if (const auto * identifier_node = ast_node->as<ASTIdentifier>())
    {
        const String & name = identifier_node->name();
        if (formal_param_names.contains(name))
            return; 

        ASTPtr ast_ptr_ident = std::const_pointer_cast<IAST>(identifier_node->shared_from_this());
        if (factory_instance.tryGet(ast_ptr_ident))
            return; 

        throw Exception(ErrorCodes::UNKNOWN_TYPE,
                        "Unknown type or type parameter '{}' in definition of user-defined type '{}'",
                        name, udt_name);
    }
    else if (const auto * data_type_node = ast_node->as<ASTDataType>())
    {
        const String & type_name_str = data_type_node->name;

        if (!data_type_node->arguments || data_type_node->arguments->children.empty())
        {
            if (formal_param_names.contains(type_name_str))
                return; 
            
            if (UserDefinedTypeFactory::instance().isTypeRegistered(type_name_str, validation_context))
                return;

            ASTPtr ast_ptr_dt = std::const_pointer_cast<IAST>(data_type_node->shared_from_this());
            if (factory_instance.tryGet(ast_ptr_dt))
                return; 

            throw Exception(ErrorCodes::UNKNOWN_TYPE,
                            "Unknown type or type parameter '{}' in definition of user-defined type '{}'",
                            type_name_str, udt_name);
        }
        else 
        {
            bool is_family_known = known_families.contains(type_name_str) ||
                                   UserDefinedTypeFactory::instance().isTypeRegistered(type_name_str, validation_context);

            if (!is_family_known)
            {
                try
                {
                    factory_instance.get(type_name_str, data_type_node->arguments);
                    is_family_known = true;
                }
                catch (const Exception & e)
                {
                    if (e.code() == ErrorCodes::UNKNOWN_TYPE)
                        is_family_known = false;
                    else
                        is_family_known = true; 
                }
            }

            if (!is_family_known)
                throw Exception(ErrorCodes::UNKNOWN_TYPE,
                                "Unknown type family '{}' in definition of user-defined type '{}'",
                                type_name_str, udt_name);

            if (data_type_node->arguments)
            {
                for (const auto & arg_child : data_type_node->arguments->children)
                {
                    validateBaseTypeRecursive(arg_child, udt_name, formal_param_names, factory_instance, validation_context, known_families);
                }
            }
        }
    }
    else if (const auto * func_node = ast_node->as<ASTFunction>())
    {
        if (func_node->arguments)
        {
            for (const auto & child : func_node->arguments->children)
            {
                validateBaseTypeRecursive(child, udt_name, formal_param_names, factory_instance, validation_context, known_families);
            }
        }
    }
    else
    {
        for (const auto & child : ast_node->children)
        {
            validateBaseTypeRecursive(child, udt_name, formal_param_names, factory_instance, validation_context, known_families);
        }
    }
}

} // Anonymous namespace


BlockIO InterpreterCreateTypeQuery::execute()
{
    const auto & create = query_ptr->as<ASTCreateTypeQuery &>();
    auto * log = &Poco::Logger::get("InterpreterCreateTypeQuery");
    LOG_DEBUG(log, "Executing CREATE TYPE query for type '{}'", create.name);
    
    auto current_context = getContext();
    current_context->checkAccess(AccessType::CREATE_TYPE);
    
    String type_name = create.name;
    
    bool is_replace = create.or_replace;
    bool type_existed_before_replace = false;

    if (UserDefinedTypeFactory::instance().isTypeRegistered(type_name, current_context))
    {
        type_existed_before_replace = true;
        LOG_DEBUG(log, "Type '{}' already exists in UserDefinedTypeFactory", type_name);
        if (create.if_not_exists && !is_replace)
        {
            LOG_DEBUG(log, "IF NOT EXISTS specified, skipping for type '{}'", type_name);
            return {};
        }
        if (is_replace)
        {
            LOG_DEBUG(log, "OR REPLACE specified, will attempt to remove old type '{}' before registration", type_name);
        }
        else
        {
            LOG_WARNING(log, "Type '{}' already exists and no OR REPLACE/IF NOT EXISTS, throwing exception", type_name);
            throw Exception(ErrorCodes::TYPE_ALREADY_EXISTS, "Type '{}' already exists", type_name);
        }
    }

    if (!create.base_type)
    {
        LOG_WARNING(log, "Base type not specified for UDT '{}', throwing exception", type_name);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Base type not specified for user-defined type '{}'", type_name);
    }

    LOG_DEBUG(log, "Base type AST provided for UDT '{}'", type_name);

    try
    {
        std::unordered_set<String> formal_param_names_set;
        if (create.type_parameters)
        {
            const auto * params_list = create.type_parameters->as<ASTExpressionList>();
            if (!params_list)
                throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Type parameters for UDT '{}' are not an expression list", type_name);
            for (const auto & param_ast : params_list->children)
            {
                const auto * param_ident = param_ast->as<ASTIdentifier>();
                if (!param_ident)
                    throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Type parameter for UDT '{}' is not an identifier", type_name);
                formal_param_names_set.insert(param_ident->name());
            }
        }

        static const std::unordered_set<String> known_type_families_set = {
            "Array", "Tuple", "Map", "Nullable", "LowCardinality",
            "AggregateFunction", "SimpleAggregateFunction", "FixedString",
            "DateTime64", "Decimal", "Enum", "Enum8", "Enum16"
        };

        validateBaseTypeRecursive(create.base_type, type_name, formal_param_names_set, DataTypeFactory::instance(), current_context, known_type_families_set);
        LOG_DEBUG(log, "Base type validation successful for UDT '{}'", type_name);
    }
    catch (const Exception & e)
    {
        LOG_WARNING(log, "Validation of base type for UDT '{}' failed: {}", type_name, e.what());
        throw; 
    }

    if (is_replace && type_existed_before_replace)
    {
        try
        {
            String delete_query_str = fmt::format("DELETE FROM system.user_defined_types WHERE name = {}", quoteString(type_name));
            ContextMutablePtr delete_query_context = Context::createCopy(current_context);
            executeQuery(delete_query_str, delete_query_context, QueryFlags{ .internal = true }, QueryProcessingStage::Complete);
            LOG_DEBUG(log, "Successfully deleted old type '{}' from system.user_defined_types due to OR REPLACE", type_name);
        }
        catch (const Exception & e)
        {
            LOG_WARNING(log, "Failed to delete type '{}' from system.user_defined_types during OR REPLACE. Error: {}. Proceeding with registration.", type_name, e.what());
        }
    }

    if (is_replace && type_existed_before_replace)
    {
        try
        {
            UserDefinedTypeFactory::instance().removeType(type_name);
            LOG_DEBUG(log, "Removed existing type '{}' from in-memory factory due to OR REPLACE.", type_name);
        }
        catch (const DB::Exception & e)
        {
            LOG_WARNING(log, "Problem removing type '{}' from in-memory factory during OR REPLACE: {}. This might happen if it was only in DB.", type_name, e.what());
        }
    }

    ASTPtr type_parameters_ast = create.type_parameters;

    String input_expression_str;
    String output_expression_str;
    String default_expression_str;
    
    if (create.input_expression)
        input_expression_str = create.input_expression->as<ASTLiteral &>().value.safeGet<String>();
    if (create.output_expression)
        output_expression_str = create.output_expression->as<ASTLiteral &>().value.safeGet<String>();
    if (create.default_expression)
        default_expression_str = create.default_expression->as<ASTLiteral &>().value.safeGet<String>();
    
    UserDefinedTypeFactory::instance().registerType(
        type_name,
        create.base_type,
        type_parameters_ast,
        input_expression_str,
        output_expression_str,
        default_expression_str);
    
    LOG_DEBUG(log, "Type '{}' successfully registered in-memory.", type_name);

    try
    {
        String base_type_ast_string = UserDefinedTypeFactory::astToString(create.base_type);
        String type_parameters_ast_string = type_parameters_ast ? UserDefinedTypeFactory::astToString(type_parameters_ast) : "";
        
        WriteBufferFromOwnString query_text_buf;
        IAST::FormatSettings format_settings(true /*one_line*/, false /*hilite*/);
        query_ptr->format(query_text_buf, format_settings);
        String create_query_string = query_text_buf.str();

        String insert_query_str = fmt::format(
            "INSERT INTO system.user_defined_types (name, base_type_ast_string, type_parameters_ast_string, input_expression, output_expression, default_expression, create_query_string) VALUES ({}, {}, {}, {}, {}, {}, {})",
            quoteString(type_name),
            quoteString(base_type_ast_string),
            type_parameters_ast_string.empty() ? "NULL" : quoteString(type_parameters_ast_string),
            input_expression_str.empty() ? "NULL" : quoteString(input_expression_str),
            output_expression_str.empty() ? "NULL" : quoteString(output_expression_str),
            default_expression_str.empty() ? "NULL" : quoteString(default_expression_str),
            quoteString(create_query_string)
        );
        
        LOG_DEBUG(log, "Persisting UDT '{}'. Executing query: {}", type_name, insert_query_str);
        ContextMutablePtr insert_query_context = Context::createCopy(current_context);
        executeQuery(insert_query_str, insert_query_context, QueryFlags{ .internal = true }, QueryProcessingStage::Complete);
        LOG_INFO(log, "Successfully persisted user-defined type '{}' to system.user_defined_types", type_name);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to persist user-defined type '{}' to system table. Error: {}. Rolling back in-memory registration.", type_name, e.what());
        try
        {
            UserDefinedTypeFactory::instance().removeType(type_name);
        }
        catch (const Exception & remove_e)
        {
            LOG_ERROR(log, "Failed to rollback in-memory registration for type '{}' after persistence error. Double error: {}", type_name, remove_e.what());
        }
        throw;
    }

    return {};
}

void registerInterpreterCreateTypeQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateTypeQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateTypeQuery", create_fn);
}

} // namespace DB
