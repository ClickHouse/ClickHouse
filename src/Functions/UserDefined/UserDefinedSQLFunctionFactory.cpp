#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Backups/RestorerFromBackup.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsBackup.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/quoteString.h>


namespace DB
{
namespace Setting
{
    extern const SettingsSetOperationMode union_default_mode;
}

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int CANNOT_DROP_FUNCTION;
    extern const int CANNOT_CREATE_RECURSIVE_FUNCTION;
    extern const int UNSUPPORTED_METHOD;
}


namespace
{
    void validateFunctionRecursiveness(const IAST & node, const String & function_to_create)
    {
        for (const auto & child : node.children)
        {
            auto function_name_opt = tryGetFunctionName(child);
            if (function_name_opt && function_name_opt.value() == function_to_create)
                throw Exception(ErrorCodes::CANNOT_CREATE_RECURSIVE_FUNCTION, "You cannot create recursive function");

            validateFunctionRecursiveness(*child, function_to_create);
        }
    }

    void validateFunction(ASTPtr function, const String & name)
    {
        ASTFunction * lambda_function = function->as<ASTFunction>();

        if (!lambda_function)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Expected function, got: {}", function->formatForErrorMessage());

        auto & lambda_function_expression_list = lambda_function->arguments->children;

        if (lambda_function_expression_list.size() != 2)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have arguments and body");

        const ASTFunction * tuple_function_arguments = lambda_function_expression_list[0]->as<ASTFunction>();

        if (!tuple_function_arguments || !tuple_function_arguments->arguments)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have valid arguments");

        std::unordered_set<String> arguments;

        for (const auto & argument : tuple_function_arguments->arguments->children)
        {
            const auto * argument_identifier = argument->as<ASTIdentifier>();

            if (!argument_identifier)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda argument must be identifier");

            const auto & argument_name = argument_identifier->name();
            auto [_, inserted] = arguments.insert(argument_name);
            if (!inserted)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Identifier {} already used as function parameter", argument_name);
        }

        ASTPtr function_body = lambda_function_expression_list[1];
        if (!function_body)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have valid function body");

        validateFunctionRecursiveness(*function_body, name);
    }

    ASTPtr normalizeCreateFunctionQuery(const IAST & create_function_query, const ContextPtr & context)
    {
        auto ptr = create_function_query.clone();
        auto & res = typeid_cast<ASTCreateFunctionQuery &>(*ptr);
        res.if_not_exists = false;
        res.or_replace = false;
        FunctionNameNormalizer::visit(res.function_core.get());
        NormalizeSelectWithUnionQueryVisitor::Data data{context->getSettingsRef()[Setting::union_default_mode]};
        NormalizeSelectWithUnionQueryVisitor{data}.visit(res.function_core);
        return ptr;
    }
}


UserDefinedSQLFunctionFactory & UserDefinedSQLFunctionFactory::instance()
{
    static UserDefinedSQLFunctionFactory result;
    return result;
}

void UserDefinedSQLFunctionFactory::checkCanBeRegistered(const ContextPtr & context, const String & function_name, const IAST & create_function_query)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);

    if (AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context)) /// NOLINT(readability-static-accessed-through-instance)
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined executable function '{}' already exists", function_name);

    validateFunction(assert_cast<const ASTCreateFunctionQuery &>(create_function_query).function_core, function_name);
}

void UserDefinedSQLFunctionFactory::checkCanBeUnregistered(const ContextPtr & context, const String & function_name)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name) ||
        AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop system function '{}'", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context)) /// NOLINT(readability-static-accessed-through-instance)
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop user defined executable function '{}'", function_name);
}

bool UserDefinedSQLFunctionFactory::registerFunction(const ContextMutablePtr & context, const String & function_name, ASTPtr create_function_query, bool throw_if_exists, bool replace_if_exists)
{
    checkCanBeRegistered(context, function_name, *create_function_query);
    create_function_query = normalizeCreateFunctionQuery(*create_function_query, context);

    try
    {
        auto & loader = context->getUserDefinedSQLObjectsStorage();
        bool stored = loader.storeObject(
            context,
            UserDefinedSQLObjectType::Function,
            function_name,
            create_function_query,
            throw_if_exists,
            replace_if_exists,
            context->getSettingsRef());
        if (!stored)
            return false;
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while storing user defined function {}", backQuote(function_name)));
        throw;
    }

    return true;
}

bool UserDefinedSQLFunctionFactory::unregisterFunction(const ContextMutablePtr & context, const String & function_name, bool throw_if_not_exists)
{
    checkCanBeUnregistered(context, function_name);

    try
    {
        auto & storage = context->getUserDefinedSQLObjectsStorage();
        bool removed = storage.removeObject(
            context,
            UserDefinedSQLObjectType::Function,
            function_name,
            throw_if_not_exists);
        if (!removed)
            return false;
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while removing user defined function {}", backQuote(function_name)));
        throw;
    }

    return true;
}

ASTPtr UserDefinedSQLFunctionFactory::get(const String & function_name) const
{
    return global_context->getUserDefinedSQLObjectsStorage().get(function_name);
}

ASTPtr UserDefinedSQLFunctionFactory::tryGet(const std::string & function_name) const
{
    return global_context->getUserDefinedSQLObjectsStorage().tryGet(function_name);
}

bool UserDefinedSQLFunctionFactory::has(const String & function_name) const
{
    return global_context->getUserDefinedSQLObjectsStorage().has(function_name);
}

std::vector<std::string> UserDefinedSQLFunctionFactory::getAllRegisteredNames() const
{
    return global_context->getUserDefinedSQLObjectsStorage().getAllObjectNames();
}

bool UserDefinedSQLFunctionFactory::empty() const
{
    return global_context->getUserDefinedSQLObjectsStorage().empty();
}

void UserDefinedSQLFunctionFactory::backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup) const
{
    backupUserDefinedSQLObjects(
        backup_entries_collector,
        data_path_in_backup,
        UserDefinedSQLObjectType::Function,
        global_context->getUserDefinedSQLObjectsStorage().getAllObjects());
}

void UserDefinedSQLFunctionFactory::restore(RestorerFromBackup & restorer, const String & data_path_in_backup)
{
    auto restored_functions = restoreUserDefinedSQLObjects(restorer, data_path_in_backup, UserDefinedSQLObjectType::Function);
    const auto & restore_settings = restorer.getRestoreSettings();
    bool throw_if_exists = (restore_settings.create_function == RestoreUDFCreationMode::kCreate);
    bool replace_if_exists = (restore_settings.create_function == RestoreUDFCreationMode::kReplace);
    auto context = restorer.getContext();
    for (const auto & [function_name, create_function_query] : restored_functions)
        registerFunction(context, function_name, create_function_query, throw_if_exists, replace_if_exists);
}

}
