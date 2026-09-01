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
#include <Poco/String.h>
#include <optional>
#include <unordered_set>
#include <Interpreters/DatabaseCatalog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_ALREADY_EXISTS;
    extern const int UNKNOWN_TYPE;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

namespace
{

void validateBaseTypeRecursive(
    const ASTPtr & ast_node,
    const String & udt_name,
    const std::unordered_set<String> & formal_param_names,
    const DataTypeFactory & factory_instance,
    ContextPtr validation_context,
    bool & references_user_defined_type)
{
    if (!ast_node)
        return;

    if (const auto * identifier_node = ast_node->as<ASTIdentifier>())
    {
        const String & name = identifier_node->name();
        if (formal_param_names.contains(name))
            return;

        if (UserDefinedTypeFactory::instance().isTypeRegistered(name, validation_context))
        {
            references_user_defined_type = true;
            return;
        }

        if (factory_instance.tryGet(ast_node))
            return;

        throw Exception(ErrorCodes::UNKNOWN_TYPE,
                        "Unknown type or type parameter '{}' in definition of user-defined type '{}'",
                        name, udt_name);
    }
    else if (const auto * data_type_node = ast_node->as<ASTDataType>())
    {
        const String & type_name_str = data_type_node->name;
        const auto arguments = data_type_node->getArguments();

        if (!arguments || arguments->children.empty())
        {
            if (formal_param_names.contains(type_name_str))
                return;

            if (UserDefinedTypeFactory::instance().isTypeRegistered(type_name_str, validation_context))
            {
                references_user_defined_type = true;
                return;
            }

            if (factory_instance.tryGet(ast_node))
                return;

            throw Exception(ErrorCodes::UNKNOWN_TYPE,
                            "Unknown type or type parameter '{}' in definition of user-defined type '{}'",
                            type_name_str, udt_name);
        }
        else
        {
            /// Ask the registry itself instead of a hard-coded list, otherwise the accepted set of
            /// families silently drifts from the families `DataTypeFactory` actually knows.
            const bool is_user_defined_family = UserDefinedTypeFactory::instance().isTypeRegistered(type_name_str, validation_context);
            if (is_user_defined_family)
                references_user_defined_type = true;

            const bool is_family_known = is_user_defined_family
                || factory_instance.hasNameOrAlias(type_name_str)
                || factory_instance.hasNameOrAlias(Poco::toLower(type_name_str));

            if (!is_family_known)
                throw Exception(ErrorCodes::UNKNOWN_TYPE,
                                "Unknown type family '{}' in definition of user-defined type '{}'",
                                type_name_str, udt_name);

            if (arguments)
            {
                for (const auto & arg_child : arguments->children)
                {
                    validateBaseTypeRecursive(arg_child, udt_name, formal_param_names, factory_instance, validation_context, references_user_defined_type);
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
                validateBaseTypeRecursive(child, udt_name, formal_param_names, factory_instance, validation_context, references_user_defined_type);
            }
        }
    }
    else
    {
        for (const auto & child : ast_node->children)
        {
            validateBaseTypeRecursive(child, udt_name, formal_param_names, factory_instance, validation_context, references_user_defined_type);
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

    /// `DataTypeFactory::getImpl` resolves user-defined types before the built-in creators, so a
    /// user-defined type named after a built-in type, alias or family would hijack every later use
    /// of that name: `CREATE TYPE UInt64 AS String` would change the meaning of `UInt64` everywhere.
    const auto & data_type_factory = DataTypeFactory::instance();
    if (data_type_factory.hasNameOrAlias(type_name) || data_type_factory.hasNameOrAlias(Poco::toLower(type_name)))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot create user-defined type {}: a built-in data type with this name already exists",
                        backQuote(type_name));

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
                /// `DataTypeFactory` substitutes the actual arguments through a map keyed by the formal
                /// parameter name, so a repeated name would silently take the value of its last
                /// occurrence: `CREATE TYPE Pair(T, T) AS Tuple(T, T)` would ignore the first argument.
                if (!formal_param_names_set.insert(param_ident->name()).second)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Duplicate type parameter '{}' in definition of user-defined type '{}'",
                                    param_ident->name(), type_name);
            }
        }

        bool references_user_defined_type = false;
        validateBaseTypeRecursive(
            create.base_type, type_name, formal_param_names_set, DataTypeFactory::instance(), current_context, references_user_defined_type);

        /// A definition with neither formal parameters nor references to other user-defined types is a
        /// complete built-in data type expression, so it can be checked exactly by instantiating it.
        /// This rejects definitions such as `Map(String)` that name a known family but are not a valid
        /// type. Definitions referencing other user-defined types are left out because their resolution
        /// in `DataTypeFactory` goes through the query context of the current thread, which is not
        /// necessarily the context this DDL query is interpreted with.
        if (formal_param_names_set.empty() && !references_user_defined_type)
            DataTypeFactory::instance().get(create.base_type);
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

    std::optional<String> input_expression_str;
    std::optional<String> output_expression_str;
    std::optional<String> default_expression_str;

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
    IAST::FormatSettings format_settings_for_storage(true /*one_line*/);
    format_settings_for_storage.show_secrets = false;
    query_ptr->format(query_text_buf, format_settings_for_storage);
    String create_query_string = query_text_buf.str();

    /// `registerType` is the atomic authority on the name: it either registers the type and persists it,
    /// or leaves the registry untouched (it rolls back its own in-memory entry when persistence fails).
    /// Rolling back here as well would let a session that lost a concurrent `CREATE TYPE` race remove the
    /// type the winning session had just created.
    udt_factory.registerType(
        current_context,
        type_name,
        create.base_type,
        type_parameters_ast,
        input_expression_str,
        output_expression_str,
        default_expression_str,
        create_query_string);

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
