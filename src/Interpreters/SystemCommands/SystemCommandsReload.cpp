#include <Interpreters/SystemCommands/SystemCommandHelpers.h>

#include <Access/AccessControl.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Common/AsynchronousMetrics.h>
#include <Common/logger_useful.h>

#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS
#include <delta_kernel_ffi.hpp>
namespace DB
{
    void tracingCallback(struct ffi::Event event);
}
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DELTA_KERNEL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

/// Sequentially tries to execute all commands and throws exception with info about failed commands
void executeCommandsAndThrowIfError(std::vector<std::function<void()>> commands)
{
    ExecutionStatus result(0);
    for (auto & command : commands)
    {
        try
        {
            command();
        }
        catch (...)
        {
            ExecutionStatus current_result = ExecutionStatus::fromCurrentException();

            if (result.code == 0)
                result.code = current_result.code;

            if (!current_result.message.empty())
            {
                if (!result.message.empty())
                    result.message += '\n';
                result.message += current_result.message;
            }
        }
    }

    if (result.code != 0)
        throw Exception::createDeprecated(result.message, result.code);
}

void reloadDictionaryFromSystemQuery(ExternalDictionariesLoader & loader, const ASTSystemQuery & query, ContextPtr context)
{
    std::string resolved_name = query.database
        ? loader.resolveDictionaryName(QualifiedTableName{query.getDatabase(), query.getTable()}, context)
        : loader.resolveDictionaryName(query.getTable(), context);

    context->onQueryExecutionStarted();
    loader.loadOrReload(resolved_name);
}

void unloadDictionaryFromSystemQuery(ExternalDictionariesLoader & loader, const ASTSystemQuery & query, ContextPtr context)
{
    if (query.database)
    {
        loader.unloadDictionary({query.getDatabase(), query.getTable()}, context);
        return;
    }

    loader.unloadDictionary(query.getTable(), context);
}

}

void registerSystemCommandsReload(SystemCommandFactory & factory)
{
    using Type = ASTSystemQuery::Type;

    factory.registerSimpleCommand(Type::RELOAD_DICTIONARY, AccessType::SYSTEM_RELOAD_DICTIONARY, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        auto & external_dictionaries_loader = context.system_context->getExternalDictionariesLoader();
        reloadDictionaryFromSystemQuery(external_dictionaries_loader, query, context.getContext());

        ExternalDictionariesLoader::resetAll();
    });

    factory.registerSimpleCommand(Type::RELOAD_DICTIONARIES, AccessType::SYSTEM_RELOAD_DICTIONARY, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.getContext()->onQueryExecutionStarted();
        executeCommandsAndThrowIfError({
            [&] { context.system_context->getExternalDictionariesLoader().reloadAllTriedToLoadInOrder(); },
            [&] { context.system_context->getEmbeddedDictionaries().reload(); }
        });
        ExternalDictionariesLoader::resetAll();
    });

    factory.registerSimpleCommand(Type::UNLOAD_DICTIONARY, AccessType::SYSTEM_RELOAD_DICTIONARY, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        auto & external_dictionaries_loader = context.system_context->getExternalDictionariesLoader();
        unloadDictionaryFromSystemQuery(external_dictionaries_loader, query, context.getContext());
        ExternalDictionariesLoader::resetAll();
    });

    factory.registerSimpleCommand(Type::UNLOAD_DICTIONARIES, AccessType::SYSTEM_RELOAD_DICTIONARY, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        auto & external_dictionaries_loader = context.system_context->getExternalDictionariesLoader();
        external_dictionaries_loader.unloadAllDictionaries();
        ExternalDictionariesLoader::resetAll();
    });

    factory.registerSimpleCommand(Type::RELOAD_FUNCTION, AccessType::SYSTEM_RELOAD_FUNCTION, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        auto & external_user_defined_executable_functions_loader = context.system_context->getExternalUserDefinedExecutableFunctionsLoader();
        external_user_defined_executable_functions_loader.reloadFunction(query.target_function);
    });

    factory.registerSimpleCommand(Type::RELOAD_FUNCTIONS, AccessType::SYSTEM_RELOAD_FUNCTION, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        auto & external_user_defined_executable_functions_loader = context.system_context->getExternalUserDefinedExecutableFunctionsLoader();
        external_user_defined_executable_functions_loader.reloadAllTriedToLoad();
    });

    /// TODO: `ON CLUSTER` requires `SYSTEM_RELOAD_DICTIONARY`, while the local check requires `SYSTEM_RELOAD_EMBEDDED_DICTIONARIES`.
    factory.registerCommand(
        Type::RELOAD_EMBEDDED_DICTIONARIES,
        globalAccessOnCluster(AccessType::SYSTEM_RELOAD_DICTIONARY),
        [](ASTSystemQuery &, SystemCommandContext & context)
        {
            context.getContext()->checkAccess(AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES);
            context.system_context->getEmbeddedDictionaries().reload();
            return BlockIO{};
        });

    factory.registerCommand(
        Type::RELOAD_CONFIG,
        globalAccessOnCluster(AccessType::SYSTEM_RELOAD_CONFIG),
        [](ASTSystemQuery &, SystemCommandContext & context)
        {
            if (context.system_context->getApplicationType() == Context::ApplicationType::LOCAL)
                throw Exception::createDeprecated("SYSTEM RELOAD CONFIG query is not supported in clickhouse-local", ErrorCodes::UNSUPPORTED_METHOD);
            context.getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            context.system_context->reloadConfig();
            return BlockIO{};
        });

    factory.registerSimpleCommand(Type::RELOAD_USERS, AccessType::SYSTEM_RELOAD_USERS, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.system_context->getAccessControl().reload(AccessControl::ReloadMode::ALL);
    });

    factory.registerSimpleCommand(Type::RELOAD_ASYNCHRONOUS_METRICS, AccessType::SYSTEM_RELOAD_ASYNCHRONOUS_METRICS, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        auto * asynchronous_metrics = context.system_context->getAsynchronousMetrics();
        if (asynchronous_metrics)
            asynchronous_metrics->update(std::chrono::system_clock::now(), /*force_update*/ true);
    });

    /// TODO: this command checks no privileges, neither locally nor `ON CLUSTER`.
    factory.registerCommand(
        Type::RELOAD_DELTA_KERNEL_TRACING,
        noAccessOnCluster(),
        [](ASTSystemQuery & query, SystemCommandContext & context) -> BlockIO
        {
#if USE_PARQUET && USE_DELTA_KERNEL_RS
            const auto & level_str = query.delta_kernel_tracing_level;
            ffi::Level level = {};

            if (level_str == "ERROR")
                level = ffi::Level::ERROR;
            else if (level_str == "WARN")
                level = ffi::Level::WARN;
            else if (level_str == "INFO")
                level = ffi::Level::INFO;
            else if (level_str == "DEBUG")
                level = ffi::Level::DEBUG;
            else if (level_str == "TRACE")
                level = ffi::Level::TRACE;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid delta kernel tracing level: {}", level_str);

            /// Reload tracing with the new level (can be called multiple times)
            bool success = ffi::enable_event_tracing(tracingCallback, level);

            if (success)
                LOG_INFO(context.log, "Delta kernel tracing level reloaded to {}", level_str);
            else
                throw Exception(ErrorCodes::DELTA_KERNEL_ERROR, "Failed to reload delta kernel tracing level to {}", level_str);

            return BlockIO{};
#else
            (void)query;
            (void)context;
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Delta Kernel support is not enabled");
#endif
        });

    factory.registerSimpleCommand(Type::RECONNECT_ZOOKEEPER, AccessType::SYSTEM_RECONNECT_ZOOKEEPER, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.getContext()->reconnectZooKeeper("triggered via SYSTEM RECONNECT ZOOKEEPER command");
    });

    factory.registerSimpleCommand(Type::RESET_DDL_WORKER, AccessType::SYSTEM_RESET_DDL_WORKER, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        context.getContext()->getDDLWorker().requestToResetState();
    });
}

}
