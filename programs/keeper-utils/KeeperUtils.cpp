#include <iostream>
#include <fstream>
#include <Columns/IColumn.h>
#include <Coordination/Changelog.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStorage.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Disks/DiskLocal.h>
#include <Formats/formatBlock.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <boost/program_options.hpp>
#include <libnuraft/nuraft.hxx>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/logger_useful.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerFormats.h>

using namespace Coordination;
using namespace DB;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SNAPSHOT;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int BAD_ARGUMENTS;
}

namespace CoordinationSetting
{
    extern const CoordinationSettingsBool compress_logs;
}

}

namespace
{

uint64_t getSnapshotPathUpToLogIdx(const String & snapshot_path)
{
    std::filesystem::path path(snapshot_path);
    std::string filename = path.stem();
    Strings name_parts;
    splitInto<'_', '.'>(name_parts, filename);
    return parse<uint64_t>(name_parts[1]);
}

void analyzeSnapshot(const std::string & snapshot_path)
{
    try
    {
        auto disk = std::make_shared<DiskLocal>("SnapshotDisk", snapshot_path);
        auto keeper_context = std::make_shared<KeeperContext>(true, std::make_shared<CoordinationSettings>());
        keeper_context->setSnapshotDisk(disk);

        // Get list of all files in the snapshot directory
        std::vector<std::string> snapshot_files;
        disk->listFiles(snapshot_path, snapshot_files);

        // Filter for snapshot files (snapshot_*.bin or snapshot_*.bin.zstd)
        std::vector<std::string> snapshot_paths;
        for (const auto & file : snapshot_files)
        {
            if (file.starts_with("snapshot_") && (file.ends_with(".bin") || file.ends_with(".bin.zstd")))
                snapshot_paths.push_back(file);
        }

        if (snapshot_paths.empty())
            throw DB::Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "No snapshot files found in {}", snapshot_path);

        // Sort snapshots by their index (newest first)
        std::sort(snapshot_paths.begin(), snapshot_paths.end(), std::greater<>());

        std::cout << "Found " << snapshot_paths.size() << " snapshots in " << snapshot_path << ":\n\n";

        for (const auto & snapshot_file : snapshot_paths)
        {
            try
            {
                std::string full_path = std::filesystem::path(snapshot_path) / snapshot_file;
                std::cout << "=== Snapshot: " << snapshot_file << " ===\n";

                // Create a snapshot manager for each snapshot
                auto snapshot_manager = KeeperSnapshotManager<KeeperMemoryStorage>(
                    std::numeric_limits<size_t>::max(), // snapshots_to_keep
                    keeper_context,
                    true, // compress_snapshots_zstd
                    "",   // superdigest
                    500   // storage_tick_time
                );

                auto result = snapshot_manager.deserializeSnapshotFromBuffer(snapshot_manager.deserializeSnapshotBufferFromDisk(getSnapshotPathUpToLogIdx(full_path)));
                if (!result.storage)
                {
                    std::cerr << "  Warning: Failed to load snapshot data\n\n";
                    continue;
                }

                // const auto & storage = *result.storage;
                const auto & snapshot_meta = result.snapshot_meta;

                std::cout << fmt::format("  Last committed log index: {}\n"
                                        "  Last committed log term: {}\n"
                                        "  Number of nodes: {}\n"
                                        "  Digest: {}\n",
                                        snapshot_meta->get_last_log_idx(),
                                        snapshot_meta->get_last_log_term(),
                                        result.storage->getNodesCount(),
                                        result.storage->getNodesDigest(/*committed=*/true, /*lock_transaction_mutex=*/false).value) << std::endl;
            }
            catch (const DB::Exception & e)
            {
                std::cerr << "  Error analyzing snapshot " << snapshot_file << ": " << e.message() << "\n\n";
            }
        }
    }
    catch (const DB::Exception & e)
    {
        throw DB::Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "Failed to analyze snapshots in {}: {}", snapshot_path, e.message());
    }
}

void analyzeChangelogs(const std::string & log_path, const std::string & specific_changelog = "")
{
    try
    {
        auto disk = std::make_shared<DiskLocal>("LogDisk", log_path);

        // Get list of all files in the log directory
        std::vector<std::string> log_files;
        disk->listFiles(log_path, log_files);

        // Filter for changelog files (changelog_*)
        std::vector<std::string> changelog_files;
        for (const auto & file : log_files)
        {
            if (file.starts_with("changelog_"))
            {
                if (specific_changelog.empty() || file == specific_changelog)
                    changelog_files.push_back(file);
            }
        }

        if (changelog_files.empty())
        {
            if (specific_changelog.empty())
                throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "No changelog files found in {}", log_path);
            else
                throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Changelog file '{}' not found in {}", specific_changelog, log_path);
        }

        // Sort changelogs by their index (newest first)
        if (specific_changelog.empty())
            std::sort(changelog_files.begin(), changelog_files.end(), std::greater<>());

        std::cout << "Found " << changelog_files.size() << " changelog files in " << log_path << ":\n\n";

        for (const auto & changelog_file : changelog_files)
        {
            try
            {
                std::string full_path = std::filesystem::path(log_path) / changelog_file;
                auto desc = Changelog::getChangelogFileDescription(full_path);

                std::cout << "=== Changelog: " << changelog_file << " ===\n"
                          << "  Path: " << desc->path << "\n"
                          << "  From log index: " << desc->from_log_index << "\n"
                          << "  To log index: " << desc->to_log_index << "\n"
                          << "  Expected entries: " << desc->expectedEntriesCountInLog() << "\n"
                          << "  Extension: " << desc->extension << "\n" << std::endl;

                if (!specific_changelog.empty())
                {
                    desc->disk = std::make_shared<DB::DiskLocal>("LogDisk", log_path);
                    CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
                    LogEntryStorage entry_storage{LogFileSettings{}, std::make_shared<KeeperContext>(true, settings)};
                    Changelog::readChangelog(desc, entry_storage);

                    for (uint64_t i = desc->from_log_index; i <= desc->to_log_index; ++i)
                    {
                        auto entry = entry_storage.getEntry(i);
                        std::cout << "    Entry: " << i << " Term: " << entry->get_term() << "\n";
                    }
                }
            }
            catch (const DB::Exception & e)
            {
                std::cerr << "  Error analyzing changelog " << changelog_file << ": " << e.message() << "\n\n";
            }
        }
    }
    catch (const DB::Exception & e)
    {
        throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to analyze changelogs in {}: {}", log_path, e.message());
    }
}

void spliceChangelogFile(
    const std::string & source_path,
    const std::string & destination_path,
    uint64_t start_index,
    uint64_t end_index)
{
    try
    {
        std::filesystem::path source_fs_path(source_path);
        std::filesystem::path dest_dir_path(destination_path);

        if (!std::filesystem::exists(source_fs_path))
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Source file {} does not exist", source_path);

        if (!std::filesystem::exists(dest_dir_path))
            std::filesystem::create_directories(dest_dir_path);
        else if (!std::filesystem::is_directory(dest_dir_path))
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Destination path {} is not a directory", destination_path);

        // Get source file description
        auto source_desc = Changelog::getChangelogFileDescription(source_path);
        source_desc->disk = std::make_shared<DB::DiskLocal>("LogDisk", source_fs_path.parent_path().string());

        if (start_index == 0)
            start_index = source_desc->from_log_index;

        if (start_index >= end_index)
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Start index must be less than end index");

        // Validate index range
        if (start_index < source_desc->from_log_index || end_index > source_desc->to_log_index + 1)
        {
            throw DB::Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Index range [{}, {}) is out of bounds for source file {} (range: [{}, {}])",
                start_index, end_index, source_path, source_desc->from_log_index, source_desc->to_log_index);
        }

        // Generate destination filename based on source filename and index range
        std::string dest_filename = Changelog::formatChangelogPath(
            "changelog", start_index, end_index - 1, source_desc->extension);
        std::filesystem::path dest_file_path = dest_dir_path / dest_filename;

        if (std::filesystem::exists(dest_file_path))
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Destination file {} already exists", dest_file_path.string());

        // Create a copy of the source descriptor with updated indices
        auto dest_desc = std::make_shared<ChangelogFileDescription>();
        dest_desc->path = dest_file_path.string();
        dest_desc->from_log_index = start_index;
        dest_desc->to_log_index = end_index - 1; // end_index is exclusive
        dest_desc->extension = source_desc->extension;
        dest_desc->disk = std::make_shared<DiskLocal>("LogDisk", dest_dir_path.string());

        // Write the spliced changelog
        std::cout << "Writing to destination: " << dest_file_path.string() << std::endl;
        Changelog::spliceChangelog(source_desc, dest_desc);
        std::cout << "Successfully created spliced changelog with entries [" << start_index << ", " << (end_index - 1) << "] in "
                  << dest_file_path.string() << std::endl;
    }
    catch (const DB::Exception & e)
    {
        throw DB::Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to splice changelog from {} to {}: {}", source_path, destination_path, e.message());
    }
}

void dumpNodes(const DB::KeeperMemoryStorage & storage, const std::string & output_file = "", const std::string & output_format = "CSVWithNamesAndTypes", bool parallel_output = false)
{
    std::ofstream out_file;
    // std::ostream * out = &std::cout;

    SharedContextHolder shared_context;
    ContextMutablePtr global_context;

    using PrintFunction = std::function<void(const std::string & key, const DB::KeeperMemoryStorage::Node & value)>;

    const auto print_nodes = [&](const PrintFunction print_function)
    {
        std::queue<std::string> keys;
        keys.push("/");
        while (!keys.empty())
        {
            auto key = keys.front();
            keys.pop();
            auto value = storage.container.getValue(key);
            print_function(key, value);
            for (const auto & child : value.getChildren())
            {
                if (key == "/")
                    keys.push(key + child.toString());
                else
                    keys.push(key + "/" + child.toString());
            }
        }
    };

    PrintFunction print_function;
    if (!output_file.empty())
    {
        out_file.open(output_file);
        if (!out_file.is_open())
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot open output file {}", output_file);

        LOG_INFO(getLogger("keeper-utils"), "Writing nodes to {}", output_file);

        shared_context = DB::Context::createShared();
        global_context = DB::Context::createGlobal(shared_context.get());
        global_context->makeGlobalContext();
        DB::registerFormats();

        ColumnsWithTypeAndName columns
        {
            {std::make_shared<DataTypeString>(), "path"},
            {std::make_shared<DataTypeInt64>(), "czxid"},
            {std::make_shared<DataTypeInt64>(), "mzxid"},
            {std::make_shared<DataTypeDateTime>(), "ctime"},
            {std::make_shared<DataTypeDateTime>(), "mtime"},
            {std::make_shared<DataTypeInt32>(), "version"},
            {std::make_shared<DataTypeInt32>(), "cversion"},
            {std::make_shared<DataTypeInt32>(), "aversion"},
            {std::make_shared<DataTypeInt64>(), "ephemeralOwner"},
            {std::make_shared<DataTypeInt32>(), "dataLength"},
            {std::make_shared<DataTypeInt32>(), "numChildren"},
            {std::make_shared<DataTypeInt64>(), "pzxid"},
            {std::make_shared<DataTypeString>(), "data"},
        };

        Block data(columns);

        auto res_columns = data.cloneEmptyColumns();
        WriteBufferFromFile output_buf(output_file);

        OutputFormatPtr output_format_processor;
        if (parallel_output)
            output_format_processor = DB::FormatFactory::instance().getOutputFormatParallelIfPossible(output_format, output_buf, data, global_context);
        else
            output_format_processor = DB::FormatFactory::instance().getOutputFormat(output_format, output_buf, data, global_context);

        print_function = [&](const auto & key, const auto & value)
        {
            size_t i = 0;
            res_columns[i++]->insert(key);
            res_columns[i++]->insert(value.stats.czxid);
            res_columns[i++]->insert(value.stats.mzxid);
            res_columns[i++]->insert(value.stats.ctime() / 1000);
            res_columns[i++]->insert(value.stats.mtime / 1000);
            res_columns[i++]->insert(value.stats.version);
            res_columns[i++]->insert(value.stats.cversion);
            res_columns[i++]->insert(value.stats.aversion);
            res_columns[i++]->insert(value.stats.ephemeralOwner());
            res_columns[i++]->insert(value.stats.data_size);
            res_columns[i++]->insert(value.stats.numChildren());
            res_columns[i++]->insert(value.stats.pzxid);
            res_columns[i++]->insert(value.getData());
        };

        print_nodes(print_function);

        data.setColumns(std::move(res_columns));
        formatBlock(output_format_processor, data);

        output_buf.finalize();
        return;
    }

    print_function = [](const auto & key, const auto & value)
    {
        std::cout << key << "\n";
        std::cout << fmt::format(
            "\tStat: {{version: {}, mtime: {}, emphemeralOwner: {}, czxid: {}, mzxid: {}, numChildren: {}, dataLength: {}}}\n",
            value.stats.version,
            value.stats.mtime,
            value.stats.ephemeralOwner(),
            value.stats.czxid,
            value.stats.mzxid,
            value.stats.numChildren(),
            value.stats.data_size);

        std::cout << "\tData: " << value.getData() << std::endl;
    };

    print_nodes(print_function);
}



int dumpStateMachine(const std::string & snapshot_path, const std::string & log_path, bool debug_mode = false, const std::string & output_file = "", const std::string & output_format = "CSVWithNamesAndTypes", bool parallel_output = false)
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("trace");

    auto logger = getLogger("keeper-utils");
    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};

    CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
    KeeperContextPtr keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", log_path));
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", snapshot_path));

    auto state_machine = std::make_shared<KeeperStateMachine<DB::KeeperMemoryStorage>>(queue, snapshots_queue, keeper_context, nullptr);
    state_machine->init();
    size_t last_committed_index = state_machine->last_commit_index();

    LOG_INFO(logger, "Last committed index: {}", last_committed_index);

    DB::KeeperLogStore changelog(
        LogFileSettings{
            .force_sync = true, .compress_logs = (*settings)[DB::CoordinationSetting::compress_logs], .rotate_interval = 10000000},
        FlushSettings(),
        keeper_context);

    changelog.init(last_committed_index, 10000000000UL); // collect all logs

    if (changelog.size() == 0)
        LOG_INFO(logger, "Changelog empty");
    else
        LOG_INFO(logger, "Last changelog entry {}", changelog.next_slot() - 1);

    // Apply all log entries to the state machine
    for (size_t i = last_committed_index + 1; i < changelog.next_slot(); ++i)
    {
        auto & entry = *changelog.entry_at(i);
        if (entry.get_val_type() == nuraft::log_val_type::app_log)
        {
            if (debug_mode)
            {
                LOG_INFO(logger, "Current digest of state machine: {}", state_machine->getNodesDigest().value);

                auto req = state_machine->parseRequest(entry.get_buf(), true);
                LOG_INFO(
                    logger,
                    "Applying log entry {}: term={}, zxid={}, session_id={}\nrequest:\n{}",
                    i,
                    entry.get_term(),
                    req->zxid,
                    req->session_id,
                    req->request->toString());
            }
            state_machine->pre_commit(i, entry.get_buf());
            state_machine->commit(i, entry.get_buf());
        }
    }

    // Dump all nodes with the specified output format
    dumpNodes(state_machine->getStorageUnsafe(), output_file, output_format, parallel_output);
    return 0;
}

} // namespace

int mainEntryClickHouseKeeperUtils(int argc, char ** argv)
{
    namespace po = boost::program_options;

    try
    {
        po::options_description global_options("Global options");
        global_options.add_options()("help,h", "Show help message")("command", po::value<std::string>(), "Command to execute (load-state)")(
            "subargs", po::value<std::vector<std::string>>(), "Arguments for command");

        po::positional_options_description pos_desc;
        pos_desc.add("command", 1).add("subargs", -1);

        po::variables_map vm;
        {
            po::parsed_options parsed
                = po::command_line_parser(argc, argv).options(global_options).positional(pos_desc).allow_unregistered().run();

            po::store(parsed, vm);
        }

        if ((vm.contains("help") && !vm.contains("command")) || !vm.contains("command"))
        {
            std::cout << "ClickHouse Keeper Utils - A utility for managing ClickHouse Keeper data\n\n"
                      << "Usage:\n  clickhouse-keeper-utils <command> [options]\n\n"
                      << "Available commands:\n"
                      << "  dump-state           Dump Keeper state from snapshot and changelog\n"
                      << "  snapshot-analyzer    Analyze Keeper snapshots and print basic information\n"
                      << "  changelog-analyzer   Analyze Keeper changelogs and print information about them\n"
                      << "  changelog-splicer    Extract a range of entries from a changelog to a new file\n\n"
                      << "Use 'clickhouse-keeper-utils <command> --help' for help with a specific command.\n";
            return 0;
        }

        std::string cmd = vm["command"].as<std::string>();

        if (cmd == "dump-state")
        {
            // Create a new command line parser for the subcommand
            po::options_description dump_state_options("Dump state options");
            dump_state_options.add_options()
                ("help,h", "Show help message")
                ("debug-mode", "Enable debug output for log entries")
                ("snapshot-path", po::value<std::string>()->required(), "Path to snapshots directory")
                ("log-path", po::value<std::string>()->required(), "Path to logs directory")
                ("output-file,o", po::value<std::string>(), "Optional: Write output to file instead of stdout")
                ("output-format,f", po::value<std::string>()->default_value("CSVWithNamesAndTypes"), "Output format (default: CSVWithNamesAndTypes)")
                ("parallel-output", po::bool_switch()->default_value(false), "Enable parallel output format processing");

            // Create a new variables map for the subcommand
            po::variables_map dump_state_vm;

            try
            {
                // Parse the remaining arguments (after the command)
                std::vector<std::string> subcommand_args;
                for (int i = 2; i < argc; ++i)
                {
                    subcommand_args.push_back(argv[i]);
                }

                // Check for help flag first
                if (std::ranges::find_if(subcommand_args, [](const std::string & s) { return s == "--help" || s == "-h"; })
                    != subcommand_args.end())
                {
                    std::cout << "Dump Keeper state from snapshot and changelog\n\n"
                              << "Usage:\n  clickhouse-keeper-utils dump-state [options]\n\n"
                              << "Options:\n"
                              << dump_state_options << "\n"
                              << "Examples:\n"
                              << "  # Output to stdout\n"
                              << "  clickhouse-keeper-utils dump-state --snapshot-path /path/to/snapshots --log-path /path/to/logs\n\n"
                              << "  # Output to file with custom format\n"
                              << "  clickhouse-keeper-utils dump-state --snapshot-path /path/to/snapshots \n"
                              << "      --log-path /path/to/logs --output-file output.txt --output-format JSONEachRow\n\n"
                              << "  # Enable parallel output processing\n"
                              << "  clickhouse-keeper-utils dump-state --snapshot-path /path/to/snapshots \n"
                              << "      --log-path /path/to/logs --output-file output.txt --parallel-output\n";
                    return 0;
                }

                // Parse the subcommand arguments
                po::parsed_options parsed = po::command_line_parser(subcommand_args).options(dump_state_options).run();
                po::store(parsed, dump_state_vm);
                po::notify(dump_state_vm);

                // Get optional output file
                std::string output_file;
                if (dump_state_vm.contains("output-file"))
                    output_file = dump_state_vm["output-file"].as<std::string>();

                // Get output format and parallel flag
                std::string output_format = dump_state_vm["output-format"].as<std::string>();
                bool parallel_output = dump_state_vm["parallel-output"].as<bool>();

                // If we get here, all required arguments are present
                return dumpStateMachine(
                    dump_state_vm["snapshot-path"].as<std::string>(),
                    dump_state_vm["log-path"].as<std::string>(),
                    dump_state_vm.contains("debug-mode") > 0,
                    output_file,
                    output_format,
                    parallel_output);
            }
            catch (const std::exception & e)
            {
                std::cerr
                    << "Error in dump-state: " << e.what() << "\n"
                    << "Usage: clickhouse-keeper-utils dump-state --snapshot-path <snapshot_path> --log-path <log_path>\n"
                    << "       [--debug-mode] [--output-file <output_file>] [--output-format <format>]\n"
                    << "       Supported formats: CSV, CSVWithNames, CSVWithNamesAndTypes, JSON, JSONEachRow, etc.\n";
                return 1;
            }
        }
        else if (cmd == "snapshot-analyzer")
        {
            po::options_description analyzer_options("Snapshot analyzer options");
            analyzer_options.add_options()
                ("help,h", "Show help message")
                ("snapshot-path", po::value<std::string>()->required(), "Path to snapshots directory");

            try
            {
                // Parse the remaining arguments (after the command)
                std::vector<std::string> subcommand_args;
                for (int i = 2; i < argc; ++i)
                    subcommand_args.push_back(argv[i]);

                // Check for help flag first
                if (std::ranges::find_if(subcommand_args, [](const std::string & s) { return s == "--help" || s == "-h"; })
                    != subcommand_args.end())
                {
                    std::cout << "Analyze Keeper snapshot and print basic information\n\n"
                              << "Usage:\n  clickhouse-keeper-utils snapshot-analyzer [options]\n\n"
                              << "Options:\n"
                              << analyzer_options << "\n"
                              << "Example:\n"
                              << "  clickhouse-keeper-utils snapshot-analyzer --snapshot-path /path/to/snapshots\n";
                    return 0;
                }


                po::variables_map analyzer_vm;
                po::store(po::command_line_parser(subcommand_args).options(analyzer_options).run(), analyzer_vm);
                po::notify(analyzer_vm);

                analyzeSnapshot(analyzer_vm["snapshot-path"].as<std::string>());
                return 0;
            }
            catch (const std::exception & e)
            {
                std::cerr << "Error in snapshot-analyzer: " << e.what() << "\n"
                          << "Usage: clickhouse-keeper-utils snapshot-analyzer --snapshot-path <snapshots_path>\n";
                return 1;
            }
        }
        else if (cmd == "changelog-analyzer")
        {
            // Create a new command line parser for the subcommand
            po::options_description analyzer_options("Changelog analyzer options");
            analyzer_options.add_options()
                ("help,h", "Show help message")
                ("log-path", po::value<std::string>()->required(), "Path to logs directory")
                ("changelog", po::value<std::string>(), "Optional: Analyze specific changelog file (e.g., changelog_1.bin)");

            try
            {
                // Parse the remaining arguments (after the command)
                std::vector<std::string> subcommand_args;
                for (int i = 2; i < argc; ++i)
                    subcommand_args.push_back(argv[i]);

                // Check for help flag first
                if (std::ranges::find_if(subcommand_args, [](const std::string & s) { return s == "--help" || s == "-h"; })
                    != subcommand_args.end())
                {
                    std::cout << "Analyze Keeper changelogs and print information about them\n\n"
                              << "Usage:\n  clickhouse-keeper-utils changelog-analyzer [options]\n\n"
                              << "Options:\n"
                              << analyzer_options << "\n"
                              << "Examples:\n"
                              << "  # Analyze all changelog files in the directory\n"
                              << "  clickhouse-keeper-utils changelog-analyzer --log-path /path/to/logs\n\n"
                              << "  # Analyze a specific changelog file\n"
                              << "  clickhouse-keeper-utils changelog-analyzer --log-path /path/to/logs --changelog changelog_1.bin\n";
                    return 0;
                }

                po::variables_map analyzer_vm;
                po::store(po::command_line_parser(subcommand_args).options(analyzer_options).run(), analyzer_vm);
                po::notify(analyzer_vm);

                std::string changelog_file;
                if (analyzer_vm.contains("changelog"))
                    changelog_file = analyzer_vm["changelog"].as<std::string>();

                analyzeChangelogs(analyzer_vm["log-path"].as<std::string>(), changelog_file);
                return 0;
            }
            catch (const std::exception & e)
            {
                std::cerr << "Error in changelog-analyzer: " << e.what() << "\n"
                          << "Usage: clickhouse-keeper-utils changelog-analyzer --log-path <logs_path> [--changelog <changelog_file>]\n";
                return 1;
            }
        }
        else if (cmd == "changelog-splicer")
        {
            // Create a new command line parser for the subcommand
            po::options_description splicer_options("Changelog splicer options");
            splicer_options.add_options()
                ("help,h", "Show help message")
                ("source", po::value<std::string>()->required(), "Path to source changelog file")
                ("destination", po::value<std::string>()->required(), "Directory where to save the output changelog file")
                ("start-index", po::value<uint64_t>(), "Start index (inclusive). If not specified, uses the source changelog's start index")
                ("end-index", po::value<uint64_t>()->required(), "End index (exclusive)");

            try
            {
                // Parse the remaining arguments (after the command)
                std::vector<std::string> subcommand_args;
                for (int i = 2; i < argc; ++i)
                    subcommand_args.push_back(argv[i]);

                // Check for help flag first
                if (std::ranges::find_if(subcommand_args, [](const std::string & s) { return s == "--help" || s == "-h"; })
                    != subcommand_args.end())
                {
                    std::cout << "Extract a range of entries from a changelog to a new file\n\n"
                              << "Usage:\n  clickhouse-keeper-utils changelog-splicer [options]\n\n"
                              << "Options:\n"
                              << splicer_options << "\n"
                              << "Examples:\n"
                              << "  # Extract specific range\n"
                              << "  clickhouse-keeper-utils changelog-splicer --source /path/to/changelog.bin \\n"
                              << "      --destination /output/dir --start-index 100 --end-index 200\n\n"
                              << "  # Extract from beginning of changelog to end-index\n"
                              << "  clickhouse-keeper-utils changelog-splicer --source /path/to/changelog.bin \\n"
                              << "      --destination /output/dir --end-index 200\n";
                    return 0;
                }

                po::variables_map splicer_vm;
                po::store(po::command_line_parser(subcommand_args).options(splicer_options).run(), splicer_vm);
                po::notify(splicer_vm);

                const auto & source_path = splicer_vm["source"].as<std::string>();
                uint64_t start_index = splicer_vm.contains("start-index")
                    ? splicer_vm["start-index"].as<uint64_t>()
                    : 0; // Will be set to actual start index in spliceChangelogFile

                spliceChangelogFile(
                    source_path,
                    splicer_vm["destination"].as<std::string>(),
                    start_index,
                    splicer_vm["end-index"].as<uint64_t>());

                return 0;
            }
            catch (const std::exception & e)
            {
                std::cerr << "Error in changelog-splicer: " << e.what() << "\n"
                          << "Usage: clickhouse-keeper-utils changelog-splicer --source <source_file> "
                          << "--destination <dest_dir> [--start-index <start>] --end-index <end>\n"
                          << "If --start-index is not provided, the source changelog's start index will be used.\n"
                          << "The destination should be a directory where the output file will be created.\n"
                          << "The output filename will be generated automatically based on the index range.\n";
                return 1;
            }
        }
        else
        {
            std::cerr << "Error: Unknown command " << cmd << "\n"
                      << "Available commands: load-state, snapshot-analyzer, changelog-analyzer, changelog-splicer\n";
            return 1;
        }
    }
    catch (...)
    {
        std::cerr << "Error: " << getCurrentExceptionMessage(/*with_stacktrace=*/true) << "\n";
        return 1;
    }
}
