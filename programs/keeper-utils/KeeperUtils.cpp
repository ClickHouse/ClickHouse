#include <iostream>
#include <unordered_map>
#include <Columns/IColumn.h>
#include <Coordination/Changelog.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStorage.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
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
    extern const int LOGICAL_ERROR;
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

void analyzeSnapshot(const std::string & snapshot_path, bool full_storage, bool with_node_stats, size_t subtrees_limit)
{
    try
    {
        auto keeper_context = std::make_shared<KeeperContext>(true, std::make_shared<CoordinationSettings>());
        std::vector<std::string> snapshot_paths;
        bool specific_snapshot_defined = false;
        if (snapshot_path.ends_with(".bin") || snapshot_path.ends_with(".bin.zstd"))
        {
            specific_snapshot_defined = true;
            snapshot_paths.push_back(snapshot_path);
            std::filesystem::path normalized_path = std::filesystem::weakly_canonical(snapshot_path).parent_path();
            auto disk = std::make_shared<DiskLocal>("SnapshotDisk", normalized_path.string());
            keeper_context->setSnapshotDisk(disk);
        }
        else
        {
            auto disk = std::make_shared<DiskLocal>("SnapshotDisk", snapshot_path);
            keeper_context->setSnapshotDisk(disk);

            // Get list of all files in the snapshot directory
            std::vector<std::string> snapshot_files;
            disk->listFiles(snapshot_path, snapshot_files);

            // Filter for snapshot files (snapshot_*.bin or snapshot_*.bin.zstd);
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
        }


        for (const auto & snapshot_file : snapshot_paths)
        {
            try
            {
                std::string full_path = specific_snapshot_defined ? snapshot_path : (std::filesystem::path(snapshot_path) / snapshot_file).generic_string();
                std::cout << "=== Snapshot: " << snapshot_file << " ===\n";

                // Create a snapshot manager for each snapshot
                auto snapshot_manager = KeeperSnapshotManager<KeeperMemoryStorage>(
                    std::numeric_limits<size_t>::max(), // snapshots_to_keep
                    keeper_context,
                    true, // compress_snapshots_zstd
                    "",   // superdigest
                    500   // storage_tick_time
                );

                auto result = snapshot_manager.deserializeSnapshotFromBuffer(
                    snapshot_manager.deserializeSnapshotBufferFromDisk(getSnapshotPathUpToLogIdx(full_path)),
                    full_storage);

                if (!result.storage)
                {
                    std::cerr << "  Warning: Failed to load snapshot data\n\n";
                    continue;
                }

                const auto & snapshot_meta = result.snapshot_meta;

                if (full_storage)
                {
                    std::cout << fmt::format(
                        "  Last committed log index: {}\n"
                        "  Last committed log term: {}\n"
                        "  Number of nodes: {}\n"
                        "  Digest: {}\n",
                        snapshot_meta->get_last_log_idx(),
                        snapshot_meta->get_last_log_term(),
                        result.storage->getNodesCount(),
                        result.storage->getNodesDigest(/*committed=*/true, /*lock_transaction_mutex=*/false).value)
                        << std::endl;
                }
                else
                {
                    std::cout << fmt::format(
                        "  Last committed log index: {}\n"
                        "  Last committed log term: {}\n"
                        "  Number of paths: {}\n",
                        snapshot_meta->get_last_log_idx(),
                        snapshot_meta->get_last_log_term(),
                        result.paths.size())
                        << std::endl;

                    if (with_node_stats)
                    {
                        std::cout << "Finding biggest subtrees... " << std::endl;
                        std::unordered_map<std::string_view, size_t> subtree_sizes;
                        for (const auto & path : result.paths)
                        {
                            if (path == "/")
                                continue;

                            std::string_view current_path = path;
                            while (true)
                            {
                                auto parent = parentNodePath(current_path);
                                if (parent == "/") // We are at the root
                                    break;

                                subtree_sizes[parent]++;
                                current_path = parent;
                            }
                        }

                        using NodeCount = std::pair<size_t, std::string_view>;
                        auto cmp = [](const NodeCount & a, const NodeCount & b) { return a.first > b.first; };
                        std::priority_queue<NodeCount, std::vector<NodeCount>, decltype(cmp)> pq(cmp);

                        for (const auto & [node_path, count] : subtree_sizes)
                        {
                            pq.emplace(count, node_path);
                            if (pq.size() > subtrees_limit)
                                pq.pop();
                        }

                        std::vector<NodeCount> top_nodes;
                        while (!pq.empty())
                        {
                            top_nodes.push_back(pq.top());
                            pq.pop();
                        }
                        std::reverse(top_nodes.begin(), top_nodes.end());

                        std::cout << fmt::format("  Top {} biggest subtrees:\n", subtrees_limit);
                        for (const auto & node : top_nodes)
                        {
                            std::cout << fmt::format("    {}: {} descendants\n", node.second, node.first);
                        }
                    }
                }
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

                    auto last_log_index = desc->from_log_index + entry_storage.size();
                    for (uint64_t i = desc->from_log_index; i < last_log_index; ++i)
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

void spliceChangelogFile(const std::string & source_path, const std::string & destination_path, uint64_t start_index, uint64_t end_index)
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
                "Index range [{}, {}) is out of changelog range [{}, {}]",
                start_index,
                end_index,
                source_desc->from_log_index,
                source_desc->to_log_index + 1);
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

void dumpSessions(const DB::KeeperMemoryStorage & storage, const std::string & output_file, const std::string & output_format, bool parallel_output)
{

    // Get session info
    auto sessions = storage.getActiveSessions();
    const auto & ephemerals = storage.committed_ephemerals;
    const auto & session_auth = storage.committed_session_and_auth;

    if (!output_file.empty())
    {
        SharedContextHolder shared_context;
        ContextMutablePtr global_context;
        shared_context = DB::Context::createShared();
        global_context = DB::Context::createGlobal(shared_context.get());
        global_context->makeGlobalContext();
        DB::registerFormats();
        WriteBufferFromFile output_buf(output_file);

        // Define output columns
        DB::ColumnsWithTypeAndName columns;
        columns.emplace_back(std::make_shared<DB::DataTypeInt64>(), "session_id");
        columns.emplace_back(std::make_shared<DB::DataTypeInt64>(), "timeout_ms");
        columns.emplace_back(std::make_shared<DB::DataTypeUInt64>(), "ephemeral_nodes_count");
        columns.emplace_back(std::make_shared<DB::DataTypeString>(), "auths");

        DB::Block data(columns);
        auto res_columns = data.cloneEmptyColumns();

        for (const auto & [session_id, timeout] : sessions)
        {
            size_t i = 0;
            res_columns[i++]->insert(session_id);
            res_columns[i++]->insert(timeout);

            // Count ephemeral nodes for this session
            size_t ephemeral_count = 0;
            if (auto it = ephemerals.find(session_id); it != ephemerals.end())
                ephemeral_count = it->second.size();
            res_columns[i++]->insert(ephemeral_count);

            if (auto it = session_auth.find(session_id); it != session_auth.end())
            {
                std::string auths;
                for (const auto & auth : it->second)
                {
                    if (!auths.empty())
                        auths += ",";
                    auths += fmt::format("{}:{}", auth.scheme, auth.id);
                }
                res_columns[i++]->insert(auths);
            }
            else
            {
                res_columns[i++]->insert("");
            }
        }

        data.setColumns(std::move(res_columns));

        // Write output
        DB::OutputFormatPtr output_format_processor;
        if (parallel_output)
            output_format_processor = DB::FormatFactory::instance().getOutputFormatParallelIfPossible(
                output_format, output_buf, data, DB::Context::getGlobalContextInstance());
        else
            output_format_processor = DB::FormatFactory::instance().getOutputFormat(
                output_format, output_buf, data, DB::Context::getGlobalContextInstance());

        formatBlock(output_format_processor, data);
        output_buf.finalize();

        return;
    }
    for (const auto & [session_id, timeout] : sessions)
    {
        std::cout << fmt::format("Session: {}\n", session_id);
        std::cout << fmt::format("\tTimeout: {}ms\n", timeout);

        // Count ephemeral nodes for this session
        size_t ephemeral_count = 0;
        if (auto it = ephemerals.find(session_id); it != ephemerals.end())
                ephemeral_count = it->second.size();
            std::cout << fmt::format("\tEphemeral nodes: {}\n", ephemeral_count);

        // Get auth info
        if (auto it = session_auth.find(session_id); it != session_auth.end() && !it->second.empty())
        {
            std::cout << "\tAuth:\n";
            for (const auto & auth : it->second)
            {
                std::cout << fmt::format("\t\t{}:{}\n", auth.scheme, auth.id);
            }
        }
        std::cout << "\n";
    }
}

void dumpNodes(const DB::KeeperMemoryStorage & storage, const std::string & output_file, const std::string & output_format, bool parallel_output, bool with_acl)
{
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
                    keys.push(fmt::format("/{}", child));
                else
                    keys.push(fmt::format("{}/{}", key, child));
            }
        }
    };

    PrintFunction print_function;
    if (!output_file.empty())
    {
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

        if (with_acl)
            columns.emplace_back(std::make_shared<DataTypeString>(), "acl");

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

            if (with_acl)
            {
                std::string acl_str;
                const auto acls = storage.acl_map.convertNumber(value.acl_id);
                for (const auto & acl : acls)
                {
                    if (!acl_str.empty())
                        acl_str += "; ";
                    acl_str += std::to_string(static_cast<int>(acl.permissions)) + ":" + acl.scheme + ":" + acl.id;
                }
                res_columns[i++]->insert(acl_str);
            }
        };

        print_nodes(print_function);

        data.setColumns(std::move(res_columns));
        formatBlock(output_format_processor, data);

        output_buf.finalize();
        return;
    }

    print_function = [&](const auto & key, const auto & value)
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

        if (with_acl)
        {
            std::string acl_str;
            const auto acls = storage.acl_map.convertNumber(value.acl_id);
            for (const auto & acl : acls)
            {
                if (!acl_str.empty())
                    acl_str += "; ";
                acl_str += std::to_string(static_cast<int>(acl.permissions)) + ":" + acl.scheme + ":" + acl.id;
            }
            std::cout << "\tACL: " << acl_str << std::endl;
        }

        std::cout << "\tData: " << value.getData() << std::endl;
    };

    print_nodes(print_function);
}

namespace
{
auto op_num_enum = std::make_shared<DataTypeEnum16>(DataTypeEnum16::Values
{
    {"Watch", 0},
    {"Close", static_cast<Int16>(Coordination::OpNum::Close)},
    {"Error", static_cast<Int16>(Coordination::OpNum::Error)},
    {"Create", static_cast<Int16>(Coordination::OpNum::Create)},
    {"Remove", static_cast<Int16>(Coordination::OpNum::Remove)},
    {"Exists", static_cast<Int16>(Coordination::OpNum::Exists)},
    {"Reconfig", static_cast<Int16>(Coordination::OpNum::Reconfig)},
    {"Get", static_cast<Int16>(Coordination::OpNum::Get)},
    {"Set", static_cast<Int16>(Coordination::OpNum::Set)},
    {"GetACL", static_cast<Int16>(Coordination::OpNum::GetACL)},
    {"SetACL", static_cast<Int16>(Coordination::OpNum::SetACL)},
    {"SimpleList", static_cast<Int16>(Coordination::OpNum::SimpleList)},
    {"Sync", static_cast<Int16>(Coordination::OpNum::Sync)},
    {"Heartbeat", static_cast<Int16>(Coordination::OpNum::Heartbeat)},
    {"List", static_cast<Int16>(Coordination::OpNum::List)},
    {"Check", static_cast<Int16>(Coordination::OpNum::Check)},
    {"Multi", static_cast<Int16>(Coordination::OpNum::Multi)},
    {"MultiRead", static_cast<Int16>(Coordination::OpNum::MultiRead)},
    {"Auth", static_cast<Int16>(Coordination::OpNum::Auth)},
    {"SessionID", static_cast<Int16>(Coordination::OpNum::SessionID)},
    {"FilteredList", static_cast<Int16>(Coordination::OpNum::FilteredList)},
    {"CheckNotExists", static_cast<Int16>(Coordination::OpNum::CheckNotExists)},
    {"CreateIfNotExists", static_cast<Int16>(Coordination::OpNum::CreateIfNotExists)},
    {"RemoveRecursive", static_cast<Int16>(Coordination::OpNum::RemoveRecursive)},
    {"CheckStat", static_cast<Int16>(Coordination::OpNum::CheckStat)},
});
}

int dumpStateMachine(
    const std::string & snapshot_path,
    const std::string & log_path,
    bool debug_mode,
    const std::string & output_file,
    const std::string & output_format,
    bool parallel_output,
    bool with_acl,
    uint64_t end_index = std::numeric_limits<uint64_t>::max(),
    bool dump_sessions = false)
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

    // Apply log entries to the state machine up to end_index
    size_t last_index_to_apply = std::min(changelog.next_slot() - 1, end_index);
    if (last_committed_index + 1 < last_index_to_apply)
    {
        LOG_INFO(logger, "Applying changelog entries from {} to {}", last_committed_index + 1, last_index_to_apply - 1);
        for (size_t i = last_committed_index + 1; i < last_index_to_apply; ++i)
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
    }

    // Dump all nodes with the specified output format and options
    // Dump nodes or sessions to output
    if (dump_sessions)
        dumpSessions(state_machine->getStorageUnsafe(), output_file, output_format, parallel_output);
    else
        dumpNodes(state_machine->getStorageUnsafe(), output_file, output_format, parallel_output, with_acl);
    return 0;
}

int deserializeChangelog(
    const std::string & changelog_path,
    const std::string & output_file,
    const std::string & output_format,
    bool parallel_output,
    bool with_requests,
    uint64_t start_index,
    uint64_t end_index)
{
    try
    {
        auto desc = Changelog::getChangelogFileDescription(changelog_path);
        desc->disk = std::make_shared<DB::DiskLocal>("LogDisk", fs::path(changelog_path).parent_path().string());

        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        LogEntryStorage entry_storage{LogFileSettings{}, std::make_shared<KeeperContext>(true, settings)};
        Changelog::readChangelog(desc, entry_storage);

        if (start_index == 0)
            start_index = desc->from_log_index;
        else if (start_index < desc->from_log_index || start_index > desc->to_log_index)
        {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "start_index {} is out of range for changelog {} which has range [{}, {}]",
                start_index,
                changelog_path,
                desc->from_log_index,
                desc->to_log_index);
        }

        if (end_index == std::numeric_limits<uint64_t>::max())
            end_index = desc->from_log_index + entry_storage.size();
        else if (end_index < desc->from_log_index || end_index > desc->from_log_index + entry_storage.size())
        {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "end_index {} is out of range for changelog {} which has range [{}, {}]",
                end_index,
                changelog_path,
                desc->from_log_index,
                desc->from_log_index + entry_storage.size());
        }

        ResponsesQueue queue(std::numeric_limits<size_t>::max());
        SnapshotsQueue snapshots_queue{1};
        KeeperContextPtr keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
        keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", fs::temp_directory_path() / "keeper-utils-log"));
        keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", fs::temp_directory_path() / "keeper-utils-snapshot"));
        auto state_machine = std::make_shared<KeeperStateMachine<DB::KeeperMemoryStorage>>(queue, snapshots_queue, keeper_context, nullptr);

        if (!output_file.empty())
        {
            LOG_INFO(getLogger("keeper-utils"), "Writing changelog entries to {}", output_file);

            SharedContextHolder shared_context;
            ContextMutablePtr global_context;
            shared_context = DB::Context::createShared();
            global_context = DB::Context::createGlobal(shared_context.get());
            global_context->makeGlobalContext();
            DB::registerFormats();

            // Define output columns
            ColumnsWithTypeAndName columns
            {
                {std::make_shared<DataTypeUInt64>(), "log_index"},
                {std::make_shared<DataTypeUInt64>(), "term"},
                {std::make_shared<DataTypeString>(), "entry_type"},
                {std::make_shared<DataTypeUInt64>(), "entry_size"},
                {std::make_shared<DataTypeUInt64>(), "entry_crc32"},
            };

            if (with_requests)
            {
                columns.push_back({std::make_shared<DataTypeUInt64>(), "session_id"});
                columns.push_back({std::make_shared<DataTypeUInt64>(), "zxid"});
                columns.push_back({std::make_shared<DataTypeDateTime64>(3), "request_timestamp"});
                columns.push_back({std::make_shared<DataTypeUInt64>(), "digest_value"});
                columns.push_back({std::make_shared<DataTypeUInt8>(), "digest_version"});
                columns.push_back({op_num_enum, "op_num"});
                columns.push_back({std::make_shared<DataTypeInt64>(), "xid"});
                columns.push_back({std::make_shared<DataTypeInt32>(), "request_idx"});
                columns.push_back({std::make_shared<DataTypeString>(), "path"});
                columns.push_back({std::make_shared<DataTypeUInt8>(), "has_watch"});
                columns.push_back({std::make_shared<DataTypeUInt64>(), "version"});
                columns.push_back({std::make_shared<DataTypeUInt8>(), "is_ephemeral"});
                columns.push_back({std::make_shared<DataTypeUInt8>(), "is_sequential"});
                columns.push_back({std::make_shared<DataTypeString>(), "data"});
            }

            Block data(columns);
            auto res_columns = data.cloneEmptyColumns();
            WriteBufferFromFile output_buf(output_file);

            OutputFormatPtr output_format_processor;
            if (parallel_output)
                output_format_processor = DB::FormatFactory::instance().getOutputFormatParallelIfPossible(
                    output_format, output_buf, data, global_context);
            else
                output_format_processor = DB::FormatFactory::instance().getOutputFormat(
                    output_format, output_buf, data, global_context);

            // Process entries in the specified range
            for (size_t i = start_index; i < end_index; ++i)
            {
                auto entry = entry_storage.getEntry(i);

                if (entry == nullptr)
                {
                    throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Log entry {} is missing", i);
                }
                size_t col_idx = 0;

                const auto add_log_entry_info = [&]()
                {
                    res_columns[col_idx++]->insert(i);
                    res_columns[col_idx++]->insert(entry->get_term());
                    res_columns[col_idx++]->insert(DB::toString(entry->get_val_type()));
                    res_columns[col_idx++]->insert(entry->get_buf().size());
                    res_columns[col_idx++]->insert(entry->get_crc32());
                };

                add_log_entry_info();
                if (with_requests)
                {
                    if (entry->get_val_type() != nuraft::log_val_type::app_log)
                    {
                        for (; col_idx < columns.size(); ++col_idx)
                            res_columns[col_idx]->insertDefault();

                        continue;
                    }

                    auto req = state_machine->parseRequest(entry->get_buf(), true);
                    auto digest = req->digest.value_or(KeeperDigest{KeeperDigestVersion::NO_DIGEST, 0});

                    const auto add_request_general_info = [&]()
                    {
                        res_columns[col_idx++]->insert(req->session_id);
                        res_columns[col_idx++]->insert(req->zxid);
                        res_columns[col_idx++]->insert(static_cast<Decimal64>(req->time));
                        res_columns[col_idx++]->insert(digest.value);
                        res_columns[col_idx++]->insert(digest.version);
                    };

                    add_request_general_info();

                    const auto xid = req->request->xid;
                    const auto add_request_info = [&](const ZooKeeperRequestPtr & request, size_t request_idx)
                    {
                        res_columns[col_idx++]->insert(request->getOpNum());
                        res_columns[col_idx++]->insert(xid);
                        res_columns[col_idx++]->insert(request_idx);
                        res_columns[col_idx++]->insert(request->getPath());
                        res_columns[col_idx++]->insert(request->has_watch);

                        /// add version
                        if (const auto * remove_request = dynamic_cast<ZooKeeperRemoveRequest *>(request.get()))
                            res_columns[col_idx++]->insert(remove_request->version);
                        else if (const auto * set_request = dynamic_cast<ZooKeeperSetRequest *>(request.get()))
                            res_columns[col_idx++]->insert(set_request->version);
                        else if (const auto * check_request = dynamic_cast<ZooKeeperCheckRequest *>(request.get()))
                            res_columns[col_idx++]->insert(check_request->version);
                        else
                            res_columns[col_idx++]->insert(0);

                        /// add create info
                        if (const auto * create_request = dynamic_cast<ZooKeeperCreateRequest *>(request.get()))
                        {
                            res_columns[col_idx++]->insert(create_request->is_ephemeral);
                            res_columns[col_idx++]->insert(create_request->is_sequential);
                        }
                        else
                        {
                            res_columns[col_idx++]->insert(0);
                            res_columns[col_idx++]->insert(0);
                        }

                        /// add data
                        if (const auto * create_request = dynamic_cast<ZooKeeperCreateRequest *>(request.get()))
                            res_columns[col_idx++]->insert(create_request->data);
                        else if (const auto * set_request = dynamic_cast<ZooKeeperSetRequest *>(request.get()))
                            res_columns[col_idx++]->insert(set_request->data);
                        else
                            res_columns[col_idx++]->insert("");
                    };

                    add_request_info(req->request, 0);

                    if (const auto * multi_request = dynamic_cast<ZooKeeperMultiRequest *>(req->request.get()))
                    {
                        for (size_t sub_request_idx = 0; sub_request_idx < multi_request->requests.size(); ++sub_request_idx)
                        {
                            col_idx = 0;
                            add_log_entry_info();
                            add_request_general_info();
                            add_request_info(multi_request->requests[sub_request_idx], sub_request_idx + 1);
                        }
                    }
                }
            }

            data.setColumns(std::move(res_columns));

            formatBlock(output_format_processor, data);
            output_buf.finalize();
            return 0;
        }

        // Process entries in the specified range
        for (size_t i = start_index; i < end_index; ++i)
        {
            auto entry = entry_storage.getEntry(i);

            std::cout << "Log Index: " << i << "\n"
                      << "Term: " << entry->get_term() << "\n"
                      << "Value Type: " << DB::toString(entry->get_val_type()) << "\n"
                      << "Data Size: " << entry->get_buf().size() << "\n"
                      << "CRC32: " << entry->get_crc32() << "\n";

            if (with_requests && entry->get_val_type() == nuraft::log_val_type::app_log)
            {
                try
                {
                    if (auto buffer = entry->get_buf_ptr(); buffer)
                    {
                        auto request = state_machine->parseRequest(*buffer, true);
                        std::cout << "Session ID: " << request->session_id << "\n"
                                  << "ZXID: " << request->zxid << "\n"
                                  << "Request timestamp: " << request->time << "\n";
                        if (request->digest)
                            std::cout << fmt::format("Digest: {} ({})\n", request->digest->value, request->digest->version);
                        std::cout << "Uses XID64: " << request->use_xid_64 << "\n"
                                  << "Request: " << request->request->toString() << "\n";
                    }
                }
                catch (const std::exception & e)
                {
                    std::cout << "Error parsing request: " << e.what() << "\n";
                }
            }

            std::cout << "------------------------------------------\n";
        }

        return 0;
    }
    catch (const std::exception & e)
    {
        std::cerr << "Error deserializing changelog: " << e.what() << std::endl;
        return 1;
    }
}

}

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
                      << "  dump-state             Dump Keeper state from snapshot and changelog\n"
                      << "  snapshot-analyzer      Analyze Keeper snapshot files\n"
                      << "  changelog-analyzer     Analyze Keeper changelog files\n"
                      << "  changelog-splicer      Extract a range of entries from a changelog\n"
                      << "  changelog-deserializer Deserialize and display changelog contents\n\n"
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
                ("parallel-output", po::bool_switch()->default_value(false), "Enable parallel output format processing")
                ("with-acl", po::bool_switch()->default_value(false), "Include ACL information in the output (only for node output)")
                ("end-index", po::value<uint64_t>()->default_value(std::numeric_limits<uint64_t>::max()), "End index (exclusive) for changelog processing")
                ("dump-sessions", po::bool_switch()->default_value(false), "Dump session information instead of node tree");

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
                              << "  # Dump node tree to stdout\n"
                              << "  clickhouse-keeper-utils dump-state --snapshot-path /path/to/snapshots --log-path /path/to/logs\n\n"
                              << "  # Dump node tree to file with custom format\n"
                              << "  clickhouse-keeper-utils dump-state --snapshot-path /path/to/snapshots \n"
                              << "      --log-path /path/to/logs --output-file nodes.txt --output-format JSONEachRow\n\n"
                              << "  # Dump session information to stdout\n"
                              << "  clickhouse-keeper-utils dump-state --snapshot-path /path/to/snapshots \n"
                              << "      --log-path /path/to/logs --dump-sessions\n\n"
                              << "  # Dump session information to file\n"
                              << "  clickhouse-keeper-utils dump-state --snapshot-path /path/to/snapshots \n"
                              << "      --log-path /path/to/logs --output-file sessions.txt --dump-sessions\n";
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

                return dumpStateMachine(
                    dump_state_vm["snapshot-path"].as<std::string>(),
                    dump_state_vm["log-path"].as<std::string>(),
                    dump_state_vm.contains("debug-mode") > 0,
                    output_file,
                    dump_state_vm["output-format"].as<std::string>(),
                    dump_state_vm["parallel-output"].as<bool>(),
                    dump_state_vm["with-acl"].as<bool>(),
                    dump_state_vm["end-index"].as<uint64_t>(),
                    dump_state_vm["dump-sessions"].as<bool>());
            }
            catch (const std::exception & e)
            {
                std::cerr
                    << "Error in dump-state: " << e.what() << "\n"
                    << "Usage: clickhouse-keeper-utils dump-state --snapshot-path <snapshot_path> --log-path <log_path>\n"
                    << "       [--debug-mode] [--output-file <output_file>] [--output-format <format>]\n"
                    << "       [--dump-sessions] [--with-acl] [--end-index <index>]\n"
                    << "\n"
                    << "When --dump-sessions is specified, the command will output session information\n"
                    << "including session IDs, timeouts, ephemeral nodes count, and watch counts.\n"
                    << "When --dump-sessions is not specified, it will output the node tree.\n"
                    << "\n"
                    << "Note: --output-format and --parallel-output are only used when --output-file is specified.\n"
                    << "      --with-acl is only used when --dump-sessions is not specified.\n";
                return 1;
            }
        }
        else if (cmd == "snapshot-analyzer")
        {
            po::options_description analyzer_options("Snapshot analyzer options");
            analyzer_options.add_options()
                ("help,h", "Show help message")
                ("snapshot-path", po::value<std::string>()->required(), "Path to snapshots directory")
                ("full-storage", po::bool_switch()->default_value(false), "Load full storage from snapshot")
                ("with-node-stats", po::bool_switch()->default_value(false), "Calculate and show subtree statistics")
                ("subtrees-limit", po::value<std::size_t>()->default_value(10), "Show top N subtrees")
            ;

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
                              << "  clickhouse-keeper-utils snapshot-analyzer --snapshot-path /path/to/snapshots\n"
                              << "  clickhouse-keeper-utils snapshot-analyzer --snapshot-path /path/to/snapshots --full-storage\n"
                              << "  clickhouse-keeper-utils snapshot-analyzer --snapshot-path /path/to/snapshots --with-node-stats\n";
                    return 0;
                }


                po::variables_map analyzer_vm;
                po::store(po::command_line_parser(subcommand_args).options(analyzer_options).run(), analyzer_vm);
                po::notify(analyzer_vm);

                analyzeSnapshot(
                    analyzer_vm["snapshot-path"].as<std::string>(),
                    analyzer_vm["full-storage"].as<bool>(),
                    analyzer_vm["with-node-stats"].as<bool>(),
                    analyzer_vm["subtrees-limit"].as<size_t>());
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
        else if (cmd == "changelog-deserializer")
        {
            // Create a new command line parser for the subcommand
            po::options_description deserializer_options("Changelog deserializer options");
            deserializer_options.add_options()
                ("help,h", "Show help message")
                ("changelog-path", po::value<std::string>()->required(), "Path to the changelog file to deserialize")
                ("output-file,o", po::value<std::string>(), "Write output to file instead of stdout")
                ("output-format,f", po::value<std::string>()->default_value("CSVWithNamesAndTypes"), "Output format (default: CSVWithNamesAndTypes)")
                ("parallel-output", po::bool_switch()->default_value(false), "Enable parallel output format processing")
                ("with-requests", po::bool_switch()->default_value(false), "Include deserialized request information")
                ("start-index", po::value<uint64_t>()->default_value(0), "Start index (inclusive) of entries to process")
                ("end-index", po::value<uint64_t>()->default_value(std::numeric_limits<uint64_t>::max()), "End index (exclusive) of entries to process");

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
                    std::cout << "Deserialize and display contents of a Keeper changelog\n\n"
                              << "Usage:\n  clickhouse-keeper-utils changelog-deserializer [options]\n\n"
                              << "Options:\n"
                              << deserializer_options << "\n"
                              << "Examples:\n"
                              << "  # Output to console\n"
                              << "  clickhouse-keeper-utils changelog-deserializer --changelog-path /path/to/changelog.bin\n\n"
                              << "  # Save to file with JSON format\n"
                              << "  clickhouse-keeper-utils changelog-deserializer --changelog-path /path/to/changelog.bin \\n"
                              << "      --output-file output.json --output-format JSONEachRow\n"
                              << "  # Process a specific range of entries\n"
                              << "  clickhouse-keeper-utils changelog-deserializer --changelog-path /path/to/changelog.bin \\n"
                              << "      --start-index 100 --end-index 200\n";
                    return 0;
                }

                po::variables_map deserializer_vm;
                po::store(po::command_line_parser(subcommand_args).options(deserializer_options).run(), deserializer_vm);
                po::notify(deserializer_vm);

                std::string output_file;
                if (deserializer_vm.contains("output-file"))
                    output_file = deserializer_vm["output-file"].as<std::string>();

                uint64_t start_idx = deserializer_vm["start-index"].as<uint64_t>();
                uint64_t end_idx = deserializer_vm["end-index"].as<uint64_t>();

                if (start_idx >= end_idx)
                    throw po::error("start-index must be less than end-index");

                return deserializeChangelog(
                    deserializer_vm["changelog-path"].as<std::string>(),
                    output_file,
                    deserializer_vm["output-format"].as<std::string>(),
                    deserializer_vm["parallel-output"].as<bool>(),
                    deserializer_vm["with-requests"].as<bool>(),
                    start_idx,
                    end_idx);
            }
            catch (const std::exception & e)
            {
                std::cerr << "Error in changelog-deserializer: " << e.what() << "\n"
                          << "Usage: clickhouse-keeper-utils changelog-deserializer --changelog-path <changelog_file> "
                          << "[--output-file <output_file>] [--output-format <format>] [--parallel-output]\n";
                return 1;
            }
        }
        else
        {
            std::cerr << "Error: Unknown command '" << cmd << "'\n"
                      << "Available commands:\n"
                      << "  dump-state          Dump Keeper state from snapshot and changelog\n"
                      << "  snapshot-analyzer   Analyze Keeper snapshot files\n"
                      << "  changelog-analyzer  Analyze Keeper changelog files\n"
                      << "  changelog-splicer   Extract a range of entries from a changelog\n"
                      << "  changelog-deserializer  Deserialize and display changelog contents\n"
                      << "\nUse 'clickhouse-keeper-utils <command> --help' for more information about a specific command.\n";
            return 1;
        }
    }
    catch (...)
    {
        std::cerr << "Error: " << getCurrentExceptionMessage(/*with_stacktrace=*/true) << "\n";
        return 1;
    }
}
