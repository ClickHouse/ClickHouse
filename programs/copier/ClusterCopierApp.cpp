#include "ClusterCopierApp.h"
#include <Common/StatusFile.h>


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
    log_level = config().getString("log-level", "trace");
    is_safe_mode = config().has("safe-mode");
    if (config().has("copy-fault-probability"))
        copy_fault_probability = std::max(std::min(config().getDouble("copy-fault-probability"), 1.0), 0.0);
    if (config().has("move-fault-probability"))
        move_fault_probability = std::max(std::min(config().getDouble("move-fault-probability"), 1.0), 0.0);
    base_dir = (config().has("base-dir")) ? config().getString("base-dir") : Poco::Path::current();


    if (config().has("experimental-use-sample-offset"))
        experimental_use_sample_offset = config().getBool("experimental-use-sample-offset");

    // process_id is '<hostname>#<start_timestamp>_<pid>'
    time_t timestamp = Poco::Timestamp().epochTime();
    auto curr_pid = Poco::Process::id();

    process_id = std::to_string(DateLUT::instance().toNumYYYYMMDDhhmmss(timestamp)) + "_" + std::to_string(curr_pid);
    host_id = escapeForFileName(getFQDNOrHostName()) + '#' + process_id;
    process_path = Poco::Path(base_dir + "/clickhouse-copier_" + process_id).absolute().toString();
    Poco::File(process_path).createDirectories();

    /// Override variables for BaseDaemon
    if (config().has("log-level"))
        config().setString("logger.level", config().getString("log-level"));

    if (config().has("base-dir") || !config().has("logger.log"))
        config().setString("logger.log", process_path + "/log.log");

    if (config().has("base-dir") || !config().has("logger.errorlog"))
        config().setString("logger.errorlog", process_path + "/log.err.log");

    Base::initialize(self);
}


void ClusterCopierApp::handleHelp(const std::string &, const std::string &)
{
    Poco::Util::HelpFormatter help_formatter(options());
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

    using Me = std::decay_t<decltype(*this)>;
    options.addOption(Poco::Util::Option("help", "", "produce this help message").binding("help")
                          .callback(Poco::Util::OptionCallback<Me>(this, &Me::handleHelp)));
}


void ClusterCopierApp::mainImpl()
{
    StatusFile status_file(process_path + "/status", StatusFile::write_full_info);
    ThreadStatus thread_status;

    auto * log = &logger();
    LOG_INFO(log, "Starting clickhouse-copier (id {}, host_id {}, path {}, revision {})", process_id, host_id, process_path, ClickHouseRevision::get());

    SharedContextHolder shared_context = Context::createShared();
    auto context = std::make_unique<Context>(Context::createGlobal(shared_context.get()));
    context->makeGlobalContext();
    SCOPE_EXIT(context->shutdown());

    context->setConfig(loaded_config.configuration);
    context->setApplicationType(Context::ApplicationType::LOCAL);
    context->setPath(process_path + "/");

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();
    registerDictionaries();
    registerDisks();

    static const std::string default_database = "_local";
    DatabaseCatalog::instance().attachDatabase(default_database, std::make_shared<DatabaseMemory>(default_database, *context));
    context->setCurrentDatabase(default_database);

    /// Initialize query scope just in case.
    CurrentThread::QueryScope query_scope(*context);

    auto copier = std::make_unique<ClusterCopier>(task_path, host_id, default_database, *context);
    copier->setSafeMode(is_safe_mode);
    copier->setCopyFaultProbability(copy_fault_probability);
    copier->setMoveFaultProbability(move_fault_probability);

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
