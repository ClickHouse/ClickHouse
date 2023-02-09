#include "ClusterCopierApp.h"
#include <Common/StatusFile.h>
#include <Common/TerminalSize.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Formats/registerFormats.h>
#include <base/scope_guard_safe.h>
#include <unistd.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

/// ClusterCopierApp

void ClusterCopierApp::initialize(Poco::Util::Application & self)
{
    is_help = config().has("help");
    if (is_help)
        return;

    config_xml_path = config().getString("config-file");
    task_path = config().getString("task-path");
    log_level = config().getString("log-level", "info");
    is_safe_mode = config().has("safe-mode");
    is_status_mode = config().has("status");
    if (config().has("copy-fault-probability"))
        copy_fault_probability = std::max(std::min(config().getDouble("copy-fault-probability"), 1.0), 0.0);
    if (config().has("move-fault-probability"))
        move_fault_probability = std::max(std::min(config().getDouble("move-fault-probability"), 1.0), 0.0);
    base_dir = (config().has("base-dir")) ? config().getString("base-dir") : fs::current_path().string();

    max_table_tries = std::max<size_t>(config().getUInt("max-table-tries", 3), 1);
    max_shard_partition_tries = std::max<size_t>(config().getUInt("max-shard-partition-tries", 3), 1);
    max_shard_partition_piece_tries_for_alter = std::max<size_t>(config().getUInt("max-shard-partition-piece-tries-for-alter", 10), 1);
    retry_delay_ms = std::chrono::milliseconds(std::max<size_t>(config().getUInt("retry-delay-ms", 1000), 100));

    if (config().has("experimental-use-sample-offset"))
        experimental_use_sample_offset = config().getBool("experimental-use-sample-offset");

    // process_id is '<hostname>#<start_timestamp>_<pid>'
    time_t timestamp = Poco::Timestamp().epochTime();
    auto curr_pid = Poco::Process::id();

    process_id = std::to_string(DateLUT::instance().toNumYYYYMMDDhhmmss(timestamp)) + "_" + std::to_string(curr_pid);
    host_id = escapeForFileName(getFQDNOrHostName()) + '#' + process_id;
    process_path = fs::weakly_canonical(fs::path(base_dir) / ("clickhouse-copier_" + process_id));
    fs::create_directories(process_path);

    /// Override variables for BaseDaemon
    if (config().has("log-level"))
        config().setString("logger.level", config().getString("log-level"));

    if (config().has("base-dir") || !config().has("logger.log"))
        config().setString("logger.log", fs::path(process_path) / "log.log");

    if (config().has("base-dir") || !config().has("logger.errorlog"))
        config().setString("logger.errorlog", fs::path(process_path) / "log.err.log");

    Base::initialize(self);
}


void ClusterCopierApp::handleHelp(const std::string &, const std::string &)
{
    uint16_t terminal_width = 0;
    if (isatty(STDIN_FILENO))
        terminal_width = getTerminalWidth();

    Poco::Util::HelpFormatter help_formatter(options());
    if (terminal_width)
        help_formatter.setWidth(terminal_width);
    help_formatter.setCommand(commandName());
    help_formatter.setHeader("Copies tables from one cluster to another");
    help_formatter.setUsage("--config-file <config-file> --task-path <task-path>");
    help_formatter.format(std::cerr);

    stopOptionsProcessing();
}


void ClusterCopierApp::defineOptions(Poco::Util::OptionSet & options)
{
    Base::defineOptions(options);

    options.addOption(Poco::Util::Option("task-path", "", "path to task in ZooKeeper")
                          .argument("task-path").binding("task-path"));
    options.addOption(Poco::Util::Option("task-file", "", "path to task file for uploading in ZooKeeper to task-path")
                          .argument("task-file").binding("task-file"));
    options.addOption(Poco::Util::Option("task-upload-force", "", "Force upload task-file even node already exists")
                          .argument("task-upload-force").binding("task-upload-force"));
    options.addOption(Poco::Util::Option("safe-mode", "", "disables ALTER DROP PARTITION in case of errors")
                          .binding("safe-mode"));
    options.addOption(Poco::Util::Option("copy-fault-probability", "", "the copying fails with specified probability (used to test partition state recovering)")
                          .argument("copy-fault-probability").binding("copy-fault-probability"));
    options.addOption(Poco::Util::Option("move-fault-probability", "", "the moving fails with specified probability (used to test partition state recovering)")
                              .argument("move-fault-probability").binding("move-fault-probability"));
    options.addOption(Poco::Util::Option("log-level", "", "sets log level")
                          .argument("log-level").binding("log-level"));
    options.addOption(Poco::Util::Option("base-dir", "", "base directory for copiers, consecutive copier launches will populate /base-dir/launch_id/* directories")
                          .argument("base-dir").binding("base-dir"));
    options.addOption(Poco::Util::Option("experimental-use-sample-offset", "", "Use SAMPLE OFFSET query instead of cityHash64(PRIMARY KEY) % n == k")
                          .argument("experimental-use-sample-offset").binding("experimental-use-sample-offset"));
    options.addOption(Poco::Util::Option("status", "", "Get for status for current execution").binding("status"));

    options.addOption(Poco::Util::Option("max-table-tries", "", "Number of tries for the copy table task")
                          .argument("max-table-tries").binding("max-table-tries"));
    options.addOption(Poco::Util::Option("max-shard-partition-tries", "", "Number of tries for the copy one partition task")
                          .argument("max-shard-partition-tries").binding("max-shard-partition-tries"));
    options.addOption(Poco::Util::Option("max-shard-partition-piece-tries-for-alter", "", "Number of tries for final ALTER ATTACH to destination table")
                          .argument("max-shard-partition-piece-tries-for-alter").binding("max-shard-partition-piece-tries-for-alter"));
    options.addOption(Poco::Util::Option("retry-delay-ms", "", "Delay between task retries")
                          .argument("retry-delay-ms").binding("retry-delay-ms"));

    using Me = std::decay_t<decltype(*this)>;
    options.addOption(Poco::Util::Option("help", "", "produce this help message").binding("help")
                          .callback(Poco::Util::OptionCallback<Me>(this, &Me::handleHelp)));
}


void ClusterCopierApp::mainImpl()
{
    /// Status command
    {
        if (is_status_mode)
        {
            SharedContextHolder shared_context = Context::createShared();
            auto context = Context::createGlobal(shared_context.get());
            context->makeGlobalContext();
            SCOPE_EXIT_SAFE(context->shutdown());

            auto zookeeper = context->getZooKeeper();
            auto status_json = zookeeper->get(task_path + "/status");

            LOG_INFO(&logger(), "{}", status_json);
            std::cout << status_json << std::endl;

            context->resetZooKeeper();
            return;
        }
    }
    StatusFile status_file(process_path + "/status", StatusFile::write_full_info);
    ThreadStatus thread_status;

    auto * log = &logger();
    LOG_INFO(log, "Starting clickhouse-copier (id {}, host_id {}, path {}, revision {})", process_id, host_id, process_path, ClickHouseRevision::getVersionRevision());

    SharedContextHolder shared_context = Context::createShared();
    auto context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();
    SCOPE_EXIT_SAFE(context->shutdown());

    context->setConfig(loaded_config.configuration);
    context->setApplicationType(Context::ApplicationType::LOCAL);
    context->setPath(process_path + "/");

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();
    registerDictionaries();
    registerDisks();
    registerFormats();

    static const std::string default_database = "_local";
    DatabaseCatalog::instance().attachDatabase(default_database, std::make_shared<DatabaseMemory>(default_database, context));
    context->setCurrentDatabase(default_database);

    /// Disable queries logging, since:
    /// - There are bits that is not allowed for global context, like adding factories info (for the query_log)
    /// - And anyway it is useless for copier.
    context->setSetting("log_queries", false);

    auto local_context = Context::createCopy(context);

    /// Initialize query scope just in case.
    CurrentThread::QueryScope query_scope(local_context);

    auto copier = std::make_unique<ClusterCopier>(
        task_path, host_id, default_database, local_context, log);
    copier->setSafeMode(is_safe_mode);
    copier->setCopyFaultProbability(copy_fault_probability);
    copier->setMoveFaultProbability(move_fault_probability);
    copier->setMaxTableTries(max_table_tries);
    copier->setMaxShardPartitionTries(max_shard_partition_tries);
    copier->setMaxShardPartitionPieceTriesForAlter(max_shard_partition_piece_tries_for_alter);
    copier->setRetryDelayMs(retry_delay_ms);
    copier->setExperimentalUseSampleOffset(experimental_use_sample_offset);

    auto task_file = config().getString("task-file", "");
    if (!task_file.empty())
        copier->uploadTaskDescription(task_path, task_file, config().getBool("task-upload-force", false));

    copier->init();
    copier->process(ConnectionTimeouts::getTCPTimeoutsWithoutFailover(context->getSettingsRef()));

    /// Reset ZooKeeper before removing ClusterCopier.
    /// Otherwise zookeeper watch can call callback which use already removed ClusterCopier object.
    context->resetZooKeeper();
}


int ClusterCopierApp::main(const std::vector<std::string> &)
{
    if (is_help)
        return 0;

    try
    {
        mainImpl();
    }
    catch (...)
    {
        tryLogCurrentException(&Poco::Logger::root(), __PRETTY_FUNCTION__);
        auto code = getCurrentExceptionCode();

        return (code) ? code : -1;
    }

    return 0;
}


}

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseClusterCopier(int argc, char ** argv)
{
    try
    {
        DB::ClusterCopierApp app;
        return app.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();

        return (code) ? code : -1;
    }
}
