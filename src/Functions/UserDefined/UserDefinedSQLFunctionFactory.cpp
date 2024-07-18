#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Backups/RestorerFromBackup.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsBackup.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
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

    ASTPtr normalizeCreateFunctionQuery(const IAST & create_function_query)
    {
        auto ptr = create_function_query.clone();
        auto & res = typeid_cast<ASTCreateFunctionQuery &>(*ptr);
        res.if_not_exists = false;
        res.or_replace = false;
        FunctionNameNormalizer().visit(res.function_core.get());
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

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User defined executable function '{}' already exists", function_name);

    validateFunction(assert_cast<const ASTCreateFunctionQuery &>(create_function_query).function_core, function_name);
}

void UserDefinedSQLFunctionFactory::checkCanBeUnregistered(const ContextPtr & context, const String & function_name)
{
    if (FunctionFactory::instance().hasNameOrAlias(function_name) ||
        AggregateFunctionFactory::instance().hasNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop system function '{}'", function_name);

    if (UserDefinedExecutableFunctionFactory::instance().has(function_name, context))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop user defined executable function '{}'", function_name);
}

bool UserDefinedSQLFunctionFactory::registerFunction(const ContextMutablePtr & context, const String & function_name, ASTPtr create_function_query, bool throw_if_exists, bool replace_if_exists)
{
    checkCanBeRegistered(context, function_name, *create_function_query);
    create_function_query = normalizeCreateFunctionQuery(*create_function_query);

    std::lock_guard lock{mutex};
    auto it = function_name_to_create_query_map.find(function_name);
    if (it != function_name_to_create_query_map.end())
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User-defined function '{}' already exists", function_name);
        else if (!replace_if_exists)
            return false;
    }

    try
    {
        auto & loader = context->getUserDefinedSQLObjectsLoader();
        bool stored = loader.storeObject(UserDefinedSQLObjectType::Function, function_name, *create_function_query, throw_if_exists, replace_if_exists, context->getSettingsRef());
        if (!stored)
            return false;
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while storing user defined function {}", backQuote(function_name)));
        throw;
    }

    function_name_to_create_query_map[function_name] = create_function_query;
    return true;
}

bool UserDefinedSQLFunctionFactory::unregisterFunction(const ContextMutablePtr & context, const String & function_name, bool throw_if_not_exists)
{
    checkCanBeUnregistered(context, function_name);

    std::lock_guard lock(mutex);
    auto it = function_name_to_create_query_map.find(function_name);
    if (it == function_name_to_create_query_map.end())
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined function '{}' doesn't exist", function_name);
        else
            return false;
    }

    try
    {
        auto & loader = context->getUserDefinedSQLObjectsLoader();
        bool removed = loader.removeObject(UserDefinedSQLObjectType::Function, function_name, throw_if_not_exists);
        if (!removed)
            return false;
    }
    catch (Exception & exception)
    {
        exception.addMessage(fmt::format("while removing user defined function {}", backQuote(function_name)));
        throw;
    }

    function_name_to_create_query_map.erase(function_name);
    return true;
}

ASTPtr UserDefinedSQLFunctionFactory::get(const String & function_name) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query_map.find(function_name);
    if (it == function_name_to_create_query_map.end())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The function name '{}' is not registered",
            function_name);

    return it->second;
}

ASTPtr UserDefinedSQLFunctionFactory::tryGet(const std::string & function_name) const
{
    std::lock_guard lock(mutex);

    auto it = function_name_to_create_query_map.find(function_name);
    if (it == function_name_to_create_query_map.end())
        return nullptr;

    return it->second;
}

bool UserDefinedSQLFunctionFactory::has(const String & function_name) const
{
    return tryGet(function_name) != nullptr;
}

std::vector<std::string> UserDefinedSQLFunctionFactory::getAllRegisteredNames() const
{
    std::vector<std::string> registered_names;

    std::lock_guard lock(mutex);
    registered_names.reserve(function_name_to_create_query_map.size());

    for (const auto & [name, _] : function_name_to_create_query_map)
        registered_names.emplace_back(name);

    return registered_names;
}

bool UserDefinedSQLFunctionFactory::empty() const
{
    std::lock_guard lock(mutex);
    return function_name_to_create_query_map.empty();
}

void UserDefinedSQLFunctionFactory::backup(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup) const
{
    backupUserDefinedSQLObjects(backup_entries_collector, data_path_in_backup, UserDefinedSQLObjectType::Function, getAllFunctions());
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

void UserDefinedSQLFunctionFactory::setAllFunctions(const std::vector<std::pair<String, ASTPtr>> & new_functions)
{
    std::unordered_map<String, ASTPtr> normalized_functions;
    for (const auto & [function_name, create_query] : new_functions)
        normalized_functions[function_name] = normalizeCreateFunctionQuery(*create_query);

    std::lock_guard lock(mutex);
    function_name_to_create_query_map = std::move(normalized_functions);
}

std::vector<std::pair<String, ASTPtr>> UserDefinedSQLFunctionFactory::getAllFunctions() const
{
    std::lock_guard lock{mutex};
    std::vector<std::pair<String, ASTPtr>> all_functions;
    all_functions.reserve(function_name_to_create_query_map.size());
    std::copy(function_name_to_create_query_map.begin(), function_name_to_create_query_map.end(), std::back_inserter(all_functions));
    return all_functions;
}

void UserDefinedSQLFunctionFactory::setFunction(const String & function_name, const IAST & create_function_query)
{
    std::lock_guard lock(mutex);
    function_name_to_create_query_map[function_name] = normalizeCreateFunctionQuery(create_function_query);
}

void UserDefinedSQLFunctionFactory::removeFunction(const String & function_name)
{
    std::lock_guard lock(mutex);
    function_name_to_create_query_map.erase(function_name);
}

void UserDefinedSQLFunctionFactory::removeAllFunctionsExcept(const Strings & function_names_to_keep)
{
    boost::container::flat_set<std::string_view> names_set_to_keep{function_names_to_keep.begin(), function_names_to_keep.end()};
    std::lock_guard lock(mutex);
    for (auto it = function_name_to_create_query_map.begin(); it != function_name_to_create_query_map.end();)
    {
        auto current = it++;
        if (!names_set_to_keep.contains(current->first))
            function_name_to_create_query_map.erase(current);
    }
}

std::unique_lock<std::recursive_mutex> UserDefinedSQLFunctionFactory::getLock() const
{
    return std::unique_lock{mutex};
}

}
