#include <Interpreters/SystemCommands/SystemCommandHelpers.h>

#include <Columns/ColumnString.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/QueryPipeline.h>
#include <base/coverage.h>
#include <Common/CoverageCollection.h>
#include <Common/ErrnoException.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTracker.h>
#include <Common/ShellCommand.h>
#include <Common/SystemAllocatedMemoryHolder.h>
#include <Common/ThreadFuzzer.h>
#include <Common/ThreadPool.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <csignal>
#include <unistd.h>

#include "config.h"

#if USE_JEMALLOC
#    include <Processors/Sources/JemallocProfileSource.h>
#    include <Common/Jemalloc.h>
#endif


namespace DB
{

namespace Setting
{
#if USE_JEMALLOC
    extern const SettingsJemallocProfileFormat jemalloc_profile_text_output_format;
    extern const SettingsBool jemalloc_profile_text_symbolize_with_inline;
#endif
}

namespace ServerSetting
{
    extern const ServerSettingsDouble cannot_allocate_thread_fault_injection_probability;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_KILL;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

#if USE_XRAY
[[clang::xray_never_instrument]] void instrumentWithXRay(bool add, ASTSystemQuery & query, const ContextPtr & context)
{
    /// query.handler_name -- handler to be set for the function
    /// query.function_name -- name of the function to be patched - rename in query to function name
    /// query.entry_type -- entry type: None, Entry or Exit
    /// query.arguments -- arguments for the handler. should be one of the following: string, int, float
    try
    {
        if (add)
        {
            InstrumentationManager::instance().patchFunction(context, query.instrumentation_function_name, query.instrumentation_handler_name, query.instrumentation_entry_type, query.instrumentation_arguments);
        }
        else
        {
            if (!query.instrumentation_subquery.empty())
            {
                auto subquery_context = Context::createCopy(context);
                subquery_context->makeQueryContext();
                subquery_context->setCurrentQueryId({});
                auto [_, block_io] = executeQuery(query.instrumentation_subquery, subquery_context, QueryFlags{ .internal = true });

                if (!block_io.pipeline.initialized())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to execute subquery");

                PullingPipelineExecutor executor(block_io.pipeline);
                Block block;

                while (executor.pull(block))
                {
                    if (block.columns() != 1)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected only one column as result of the subquery, but {} were found", block.columns());

                    const auto & column_with_type = block.getByPosition(0);
                    const auto & column = column_with_type.column;
                    if (!isUInt(column_with_type.type))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected non-UInt column as result of the subquery: {}", column_with_type.type);

                    for (size_t i = 0; i < column->size(); ++i)
                    {
                        UInt64 id = column->getUInt(i);
                        InstrumentationManager::instance().unpatchFunction(id);
                    }
                }
            }
            else
            {
                InstrumentationManager::instance().unpatchFunction(query.instrumentation_point.value());
            }
        }
    }
    catch (const DB::Exception & e)
    {
        String id;
        if (query.instrumentation_point.has_value())
        {
            if (std::holds_alternative<Instrumentation::All>(query.instrumentation_point.value()))
                id = " and ALL ids";
            else if (std::holds_alternative<String>(query.instrumentation_point.value()))
                id = fmt::format(" and ids with function_name containing '{}'", std::get<String>(query.instrumentation_point.value()));
            else
                id = fmt::format(" and id '{}'", std::get<UInt64>(query.instrumentation_point.value()));
        }

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Failed to instrument function '{}' with handler '{}', entry type '{}'{}: {}",
            query.instrumentation_function_name,
            query.instrumentation_handler_name,
            Instrumentation::entryTypeToString(query.instrumentation_entry_type),
            id,
            e.what());
    }
}
#endif

}

void registerSystemCommandsServer(SystemCommandFactory & factory)
{
    using Type = ASTSystemQuery::Type;

    factory.registerSimpleCommand(Type::SHUTDOWN, AccessType::SYSTEM_SHUTDOWN, [](ASTSystemQuery &, SystemCommandContext &)
    {
        if (kill(0, SIGTERM))
            throw ErrnoException(ErrorCodes::CANNOT_KILL, "System call kill(0, SIGTERM) failed");
    });

    factory.registerSimpleCommand(Type::KILL, AccessType::SYSTEM_SHUTDOWN, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        /// Exit with the same code as it is usually set by shell when process is terminated by SIGKILL.
        /// It's better than doing 'raise' or 'kill', because they have no effect for 'init' process (with pid = 0, usually in Docker).
        LOG_INFO(context.log, "Exit immediately as the SYSTEM KILL command has been issued.");
        _exit(128 + SIGKILL);
    });

    factory.registerSimpleCommand(Type::SUSPEND, AccessType::SYSTEM_SHUTDOWN, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        auto command = fmt::format("kill -STOP {0} && sleep {1} && kill -CONT {0}", getpid(), query.seconds);
        LOG_DEBUG(context.log, "Will run {}", command);
        auto res = ShellCommand::execute(command);
        res->in.close();
        WriteBufferFromOwnString out;
        copyData(res->out, out);
        copyData(res->err, out);
        if (!out.str().empty())
            LOG_DEBUG(context.log, "The command {} returned output: {}", command, out.str());
        res->wait();
    });

    factory.registerSimpleCommand(Type::STOP_LISTEN, AccessType::SYSTEM_LISTEN, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        context.getContext()->stopServers(query.server_type);
    });

    factory.registerSimpleCommand(Type::START_LISTEN, AccessType::SYSTEM_LISTEN, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        context.getContext()->startServers(query.server_type);
    });

    factory.registerSimpleCommand(Type::STOP_THREAD_FUZZER, AccessType::SYSTEM_THREAD_FUZZER, [](ASTSystemQuery &, SystemCommandContext &)
    {
        ThreadFuzzer::stop();
        CannotAllocateThreadFaultInjector::setFaultProbability(0);
    });

    factory.registerSimpleCommand(Type::START_THREAD_FUZZER, AccessType::SYSTEM_THREAD_FUZZER, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        ThreadFuzzer::start();
        CannotAllocateThreadFaultInjector::setFaultProbability(context.getContext()->getServerSettings()[ServerSetting::cannot_allocate_thread_fault_injection_probability]);
    });

    /// The parser cases of the failpoint statements and of SYSTEM SET COVERAGE TEST never read an
    /// ON CLUSTER clause, so those cluster spellings do not parse and reach no host.
    auto register_local_command = [&](Type type, AccessType access_type, SystemCommandFactory::ActionFn action)
    {
        factory.registerCommand(
            type,
            noAccessOnCluster(),
            [access_type, action_fn = std::move(action)](ASTSystemQuery & query, SystemCommandContext & context)
            {
                context.getContext()->checkAccess(access_type);
                action_fn(query, context);
                return BlockIO{};
            });
    };

    /// Outside the `USE_LIBFIU` guard below on purpose: this statement asks for a
    /// server that injects nothing, which a build without libfiu already is. Failing
    /// it would only make every caller - a test harness, above all - special-case a
    /// build flag to ask for a state that already holds.
    register_local_command(Type::DISABLE_ALL_FAILPOINTS, AccessType::SYSTEM_FAILPOINT, [](ASTSystemQuery &, SystemCommandContext &)
    {
        FailPointInjection::disableAllFailPoints();
    });

#if USE_LIBFIU
    register_local_command(Type::ENABLE_FAILPOINT, AccessType::SYSTEM_FAILPOINT, [](ASTSystemQuery & query, SystemCommandContext &)
    {
        FailPointInjection::enableFailPoint(query.fail_point_name);
    });

    register_local_command(Type::DISABLE_FAILPOINT, AccessType::SYSTEM_FAILPOINT, [](ASTSystemQuery & query, SystemCommandContext &)
    {
        FailPointInjection::disableFailPoint(query.fail_point_name);
    });

    register_local_command(Type::WAIT_FAILPOINT, AccessType::SYSTEM_FAILPOINT, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        context.getContext()->onQueryExecutionStarted();
        if (query.fail_point_action == ASTSystemQuery::FailPointAction::PAUSE)
        {
            LOG_TRACE(context.log, "Waiting for failpoint {} to pause", query.fail_point_name);
            FailPointInjection::waitForPause(query.fail_point_name);
            LOG_TRACE(context.log, "Failpoint {} has paused", query.fail_point_name);
        }
        else
        {
            LOG_TRACE(context.log, "Waiting for failpoint {} to resume", query.fail_point_name);
            FailPointInjection::waitForResume(query.fail_point_name);
            LOG_TRACE(context.log, "Failpoint {} has resumed", query.fail_point_name);
        }
    });

    register_local_command(Type::NOTIFY_FAILPOINT, AccessType::SYSTEM_FAILPOINT, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        LOG_TRACE(context.log, "Notifying failpoint {}", query.fail_point_name);
        FailPointInjection::notifyFailPoint(query.fail_point_name);
        LOG_TRACE(context.log, "Notified failpoint {}", query.fail_point_name);
    });

    factory.registerSimpleCommand(Type::ALLOCATE_MEMORY, AccessType::SYSTEM_MEMORY, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        auto holder = context.getContext()->getSystemAllocatedMemoryHolder();
        if (!holder)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SYSTEM ALLOCATE MEMORY is not enabled");
        holder->alloc(query.untracked_memory_size);
        LOG_DEBUG(context.log, "Total tracked memory is {}", ReadableSize(total_memory_tracker.get()));
    });

    factory.registerSimpleCommand(Type::FREE_MEMORY, AccessType::SYSTEM_MEMORY, [](ASTSystemQuery &, SystemCommandContext & context)
    {
        auto holder = context.getContext()->getSystemAllocatedMemoryHolder();
        if (!holder)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SYSTEM ALLOCATE MEMORY is not enabled");
        holder->free();
        LOG_DEBUG(context.log, "Total tracked memory is {}", ReadableSize(total_memory_tracker.get()));
    });
#else
    for (const auto type : {Type::ENABLE_FAILPOINT, Type::DISABLE_FAILPOINT, Type::WAIT_FAILPOINT, Type::NOTIFY_FAILPOINT})
    {
        factory.registerCommand(type, noAccessOnCluster(), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
        {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without FIU support");
        });
    }

    for (const auto type : {Type::ALLOCATE_MEMORY, Type::FREE_MEMORY})
    {
        factory.registerCommand(type, globalAccessOnCluster(AccessType::SYSTEM_MEMORY), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
        {
            throwUnknownSystemQueryType();
        });
    }
#endif

    factory.registerSimpleCommand(Type::RESET_COVERAGE, AccessType::SYSTEM, [](ASTSystemQuery &, SystemCommandContext &)
    {
        resetCoverage();
    });

    register_local_command(Type::SET_COVERAGE_TEST, AccessType::SYSTEM, [](ASTSystemQuery & query, [[maybe_unused]] SystemCommandContext & context)
    {
        LOG_INFO(getLogger("InterpreterSystemQuery"),
            "SYSTEM SET COVERAGE TEST '{}' received", query.coverage_test_name);
#if WITH_COVERAGE_DEPTH
#if defined(__ELF__) && !defined(OS_FREEBSD)
        /// The process writes its coverage to files, see `initCoverageFromEnvironment`.
        if (!isCoverageFileSinkEnabled())
#endif
        {
            /// Register (or re-register) the flush callback so coverage data is
            /// resolved and inserted into system.coverage_log when the previous
            /// test's counters are flushed.  We re-register on each call so the
            /// captured global context stays fresh after server restart scenarios,
            /// and so that the callback is available even on the very first call.
            ContextPtr global_ctx = context.getContext()->getGlobalContext();
            registerCoverageFlushCallback(
                [global_ctx](std::string_view prev_test,
                             const std::vector<CovCounter> & name_refs,
                             const std::vector<IndirectCallEntry> & indirect_calls)
                {
                    LOG_INFO(getLogger("CoverageCollection"),
                        "Flushing coverage for test '{}': {} covered counters, {} indirect calls",
                        prev_test, name_refs.size(), indirect_calls.size());
#if defined(__ELF__) && !defined(OS_FREEBSD)
                    DB::collectAndInsertCoverage(prev_test, name_refs, indirect_calls, global_ctx);
#else
                    (void)prev_test;
                    (void)name_refs;
                    (void)indirect_calls;
                    (void)global_ctx;
#endif
                });
        }
#endif
        setCoverageTest(query.coverage_test_name);
    });

#if USE_XRAY
    factory.registerSimpleCommand(Type::INSTRUMENT_ADD, AccessType::SYSTEM_INSTRUMENT_ADD, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        instrumentWithXRay(true, query, context.getContext());
    });

    factory.registerSimpleCommand(Type::INSTRUMENT_REMOVE, AccessType::SYSTEM_INSTRUMENT_REMOVE, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        instrumentWithXRay(false, query, context.getContext());
    });
#else
    factory.registerCommand(Type::INSTRUMENT_ADD, globalAccessOnCluster(AccessType::SYSTEM_INSTRUMENT_ADD), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without XRay support");
    });

    factory.registerCommand(Type::INSTRUMENT_REMOVE, globalAccessOnCluster(AccessType::SYSTEM_INSTRUMENT_REMOVE), [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without XRay support");
    });
#endif

    const auto jemalloc_access_on_cluster = globalAccessOnCluster(AccessType::SYSTEM_JEMALLOC);
#if USE_JEMALLOC
    factory.registerSimpleCommand(Type::JEMALLOC_PURGE, AccessType::SYSTEM_JEMALLOC, [](ASTSystemQuery &, SystemCommandContext &)
    {
        Jemalloc::purgeArenas();
    });

    factory.registerCommand(Type::JEMALLOC_ENABLE_PROFILE, jemalloc_access_on_cluster, [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Queries for enabling/disabling global profiler are deprecated. Please use config 'jemalloc_enable_global_profiler' or "
            "enable it per query using setting 'jemalloc_enable_profiler'");
    });

    /// TODO: this should fail with the same deprecation error as `JEMALLOC_ENABLE_PROFILE`.
    factory.registerCommand(Type::JEMALLOC_DISABLE_PROFILE, jemalloc_access_on_cluster, [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
    {
        throwUnknownSystemQueryType();
    });

    factory.registerCommand(Type::JEMALLOC_FLUSH_PROFILE, jemalloc_access_on_cluster, [](ASTSystemQuery &, SystemCommandContext & command_context)
    {
        auto context = command_context.getContext();
        context->checkAccess(AccessType::SYSTEM_JEMALLOC);
        auto filename = std::string(Jemalloc::flushProfile("/tmp/jemalloc_clickhouse"));
        auto format = context->getSettingsRef()[Setting::jemalloc_profile_text_output_format];
        auto symbolize_with_inline = context->getSettingsRef()[Setting::jemalloc_profile_text_symbolize_with_inline];

        std::string output_filename;
        if (format == JemallocProfileFormat::Raw)
        {
            output_filename = filename;
        }
        else if (format == JemallocProfileFormat::Symbolized)
        {
            output_filename = filename + ".symbolized";
            symbolizeJemallocHeapProfile(filename, output_filename, format, symbolize_with_inline);
        }
        else
        {
            output_filename = filename + ".collapsed";
            symbolizeJemallocHeapProfile(filename, output_filename, format, symbolize_with_inline);
        }
        auto col = ColumnString::create();
        col->insertData(output_filename.data(), output_filename.size());
        Columns columns;
        columns.emplace_back(std::move(col));
        Chunk chunk(std::move(columns), 1);
        SharedHeader header = std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "filename")});
        auto filename_source = std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk));
        BlockIO result;
        result.pipeline = QueryPipeline(filename_source);
        return result;
    });
#else
    for (const auto type : {Type::JEMALLOC_PURGE, Type::JEMALLOC_ENABLE_PROFILE, Type::JEMALLOC_DISABLE_PROFILE, Type::JEMALLOC_FLUSH_PROFILE})
    {
        factory.registerCommand(type, jemalloc_access_on_cluster, [](ASTSystemQuery &, SystemCommandContext &) -> BlockIO
        {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without JEMalloc");
        });
    }
#endif
}

}
