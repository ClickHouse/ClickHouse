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
#include <Interpreters/DatabaseCatalog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_ALREADY_EXISTS;
    extern const int UNKNOWN_TYPE;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int BAD_ARGUMENTS;
}

namespace
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

}


BlockIO InterpreterCreateTypeQuery::execute()
{
    const auto & create = query_ptr->as<ASTCreateTypeQuery &>();
    auto * log = &Poco::Logger::get("InterpreterCreateTypeQuery");

    auto current_context = getContext();
    current_context->checkAccess(AccessType::CREATE_TYPE);
    
    auto & udt_factory = UserDefinedTypeFactory::instance();
    
    String type_name = create.name;
    
    bool is_replace = create.or_replace;
    bool type_existed_before_replace = false;

    if (udt_factory.isTypeRegistered(type_name, current_context))
    {
        type_existed_before_replace = true;
        if (create.if_not_exists && !is_replace)
        {
            return {};
        }
        if (is_replace)
        {
            // Will remove existing type before creating new one
        }
        else
        {
            throw Exception(ErrorCodes::TYPE_ALREADY_EXISTS, "Type '{}' already exists", type_name);
        }
    }

    if (!create.base_type)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Base type not specified for user-defined type '{}'", type_name);
    }

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
            udt_factory.removeType(current_context, type_name); 
        }
        catch (const DB::Exception & e)
        {
            LOG_WARNING(log, "Problem removing type '{}' during OR REPLACE: {}. Proceeding with registration.", type_name, e.what());
        }
    }

    ASTPtr type_parameters_ast = create.type_parameters;

    String input_expression_str;
    String output_expression_str;
    String default_expression_str;
    
    if (create.input_expression)
    {
        const auto * lit = create.input_expression->as<ASTLiteral>();
        if (!lit) throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Input expression must be a string literal");
        input_expression_str = lit->value.safeGet<String>();
    }
    if (create.output_expression)
    {
        const auto * lit = create.output_expression->as<ASTLiteral>();
        if (!lit) throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Output expression must be a string literal");
        output_expression_str = lit->value.safeGet<String>();
    }
    if (create.default_expression)
    {
        const auto * lit = create.default_expression->as<ASTLiteral>();
        if (!lit) throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Default expression must be a string literal");
        default_expression_str = lit->value.safeGet<String>();
    }

    WriteBufferFromOwnString query_text_buf;
    IAST::FormatSettings format_settings_for_storage(true /*one_line*/, false /*hilite=off for storage*/); 
    format_settings_for_storage.show_secrets = false;
    query_ptr->format(query_text_buf, format_settings_for_storage);
    String create_query_string = query_text_buf.str();
    
    try
    {
        udt_factory.registerType(
            current_context, 
            type_name,
            create.base_type,
            type_parameters_ast,
            input_expression_str,
            output_expression_str,
            default_expression_str,
            create_query_string);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to register user-defined type '{}'. Error: {}", type_name, e.what());
        if (udt_factory.isTypeRegistered(type_name, current_context))
        {
            try
            {
                udt_factory.removeType(current_context, type_name); 
            }
            catch (const DB::Exception & remove_e)
            {
                LOG_ERROR(log, "Failed to rollback in-memory registration for type '{}' after persistence error. Double error: {}", type_name, remove_e.what());
            }
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

}
