#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverInvoker.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverRegistry.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverUtils.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/ASTCreateFunctionWithDriverQuery.h>
#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>

#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

namespace
{
    namespace DriverUtils = UserDefinedExecutableFunctionDriverUtils;

    void handleDriverBasedDrop(
        const ASTCreateFunctionWithDriverQuery & create_query,
        const ContextPtr & current_context)
    {
        auto log = getLogger("InterpreterDropFunctionQuery");
        const String function_name = create_query.getFunctionName();

        auto driver = UserDefinedExecutableFunctionDriverRegistry::instance().tryGet(create_query.engine_name);
        if (!driver)
            LOG_WARNING(log, "Driver '{}' for function '{}' is not registered; skipping driver drop_command",
                create_query.engine_name, function_name);

        String dynamic_dir = current_context->getDynamicUserDefinedExecutableFunctionsPath();
        if (!dynamic_dir.empty() && !dynamic_dir.ends_with('/'))
            dynamic_dir.push_back('/');

        const String escaped = escapeForFileName(function_name);
        const String working_dir = dynamic_dir.empty() ? String() : DriverUtils::readDriverWorkingDirectory(current_context, function_name);

        if (driver)
        {
            std::vector<std::pair<String, String>> engine_argument_values;
            for (const auto & [name, value_ast] : create_query.engine_arguments)
            {
                const auto * literal = value_ast->as<ASTLiteral>();
                if (!literal)
                    continue;
                engine_argument_values.emplace_back(name, DriverUtils::engineArgumentToString(*literal));
            }

            try
            {
                UserDefinedExecutableFunctionDriverInvoker::runDropCommand(
                    *driver, function_name,
                    DriverUtils::formatReturnType(create_query.return_type_ast),
                    DriverUtils::formatArgsSignature(create_query.arguments_ast),
                    working_dir,
                    engine_argument_values);
            }
            catch (...)
            {
                tryLogCurrentException(log, "while running driver drop_command");
            }
        }

        if (!dynamic_dir.empty())
        {
            LOG_INFO(log, "Removing dynamic config and working directory {} of driver-created function '{}' from {}",
                working_dir.empty() ? "(none)" : working_dir, function_name, dynamic_dir);

            auto remove_logged = [&](const String & path, bool recursive)
            {
                std::error_code ec;
                if (recursive)
                    std::filesystem::remove_all(path, ec);
                else
                    std::filesystem::remove(path, ec);
                if (ec)
                    LOG_WARNING(log, "Cannot remove {}: {}", path, ec.message());
            };

            remove_logged(dynamic_dir + escaped + ".xml", /*recursive=*/ false);
            remove_logged(dynamic_dir + escaped + ".yaml", /*recursive=*/ false);
            remove_logged(DriverUtils::driverWorkingDirectoryMetadataPath(current_context, function_name), /*recursive=*/ false);
            if (!working_dir.empty())
                remove_logged(working_dir, /*recursive=*/ true);
        }

        /// Tell the executable UDF loader to drop the function now that its config is gone.
        try
        {
            const auto & loader = current_context->getExternalUserDefinedExecutableFunctionsLoader();
            loader.reloadConfig();
        }
        catch (...)
        {
            tryLogCurrentException(log, "while reloading external UDF loader after drop");
        }
    }
}

BlockIO InterpreterDropFunctionQuery::execute()
{
    FunctionNameNormalizer::visit(query_ptr.get());

    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    ASTDropFunctionQuery & drop_function_query = updated_query_ptr->as<ASTDropFunctionQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    auto current_context = getContext();

    if (!drop_function_query.cluster.empty())
    {
        if (current_context->getUserDefinedSQLObjectsStorage().isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because used-defined functions are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(updated_query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    bool throw_if_not_exists = !drop_function_query.if_exists;

    /// If the function was created by a driver, invoke the driver's drop_command and remove its config.
    if (auto existing_ast = current_context->getUserDefinedSQLObjectsStorage().tryGet(drop_function_query.function_name))
    {
        if (const auto * driver_ast = existing_ast->as<ASTCreateFunctionWithDriverQuery>())
        {
            handleDriverBasedDrop(*driver_ast, current_context);
            current_context->getUserDefinedSQLObjectsStorage().removeObject(
                current_context,
                UserDefinedSQLObjectType::Function,
                drop_function_query.function_name,
                throw_if_not_exists);
            return {};
        }
    }

    UserDefinedSQLFunctionFactory::instance().unregisterFunction(current_context, drop_function_query.function_name, throw_if_not_exists);

    return {};
}

void registerInterpreterDropFunctionQuery(InterpreterFactory & factory);
void registerInterpreterDropFunctionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropFunctionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropFunctionQuery", create_fn);
}

}
