#include <LogRunner.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <unordered_map>
#include <unordered_set>

#include <Columns/IColumn.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperStorage.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <Disks/DiskLocal.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/registerFormats.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/IInputFormat.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Exception.h>
#include <Common/InterruptListener.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/scope_guard_safe.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace DB::Setting
{
    extern const SettingsNonZeroUInt64 max_block_size;
}

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

struct ZooKeeperRequestBlock
{
    explicit ZooKeeperRequestBlock(DB::Block block_)
        : block(std::move(block_))
        , hostname_idx(block.getPositionByName("hostname"))
        , request_event_time_idx(block.getPositionByName("request_event_time"))
        , thread_id_idx(block.getPositionByName("thread_id"))
        , session_id_idx(block.getPositionByName("session_id"))
        , xid_idx(block.getPositionByName("xid"))
        , has_watch_idx(block.getPositionByName("has_watch"))
        , op_num_idx(block.getPositionByName("op_num"))
        , path_idx(block.getPositionByName("path"))
        , data_idx(block.getPositionByName("data"))
        , is_ephemeral_idx(block.getPositionByName("is_ephemeral"))
        , is_sequential_idx(block.getPositionByName("is_sequential"))
        , response_event_time_idx(block.getPositionByName("response_event_time"))
        , error_idx(block.getPositionByName("error"))
        , requests_size_idx(block.getPositionByName("requests_size"))
        , version_idx(block.getPositionByName("version"))
    {}

    size_t rows() const
    {
        return block.rows();
    }

    UInt64 getExecutorId(size_t row) const
    {
        return getSessionId(row);
    }

    DB::DateTime64 getRequestEventTime(size_t row) const
    {
        return getField(request_event_time_idx, row).safeGet<DB::DateTime64>();
    }

    DB::DateTime64 getResponseEventTime(size_t row) const
    {
        return getField(response_event_time_idx, row).safeGet<DB::DateTime64>();
    }

    Int64 getSessionId(size_t row) const
    {
        return getField(session_id_idx, row).safeGet<Int64>();
    }

    bool hasWatch(size_t row) const
    {
        return getField(has_watch_idx, row).safeGet<UInt8>();
    }

    Coordination::OpNum getOpNum(size_t row) const
    {
        return static_cast<Coordination::OpNum>(getField(op_num_idx, row).safeGet<Int64>());
    }

    bool isEphemeral(size_t row) const
    {
        return getField(is_ephemeral_idx, row).safeGet<UInt8>();
    }

    bool isSequential(size_t row) const
    {
        return getField(is_sequential_idx, row).safeGet<UInt8>();
    }

    std::string getPath(size_t row) const
    {
        return getField(path_idx, row).safeGet<std::string>();
    }

    std::string getData(size_t row) const
    {
        return getField(data_idx, row).safeGet<std::string>();
    }

    UInt64 getRequestsSize(size_t row) const
    {
        return getField(requests_size_idx, row).safeGet<UInt64>();
    }

    std::optional<Int32> getVersion(size_t row) const
    {
        auto field = getField(version_idx, row);
        if (field.isNull())
            return std::nullopt;
        return static_cast<Int32>(field.safeGet<Int64>());
    }

    std::optional<Coordination::Error> getError(size_t row) const
    {
        auto field = getField(error_idx, row);
        if (field.isNull())
            return std::nullopt;

        return static_cast<Coordination::Error>(field.safeGet<Int64>());
    }
private:
    DB::Field getField(size_t position, size_t row) const
    {
        DB::Field field;
        block.getByPosition(position).column->get(row, field);
        return field;
    }

    DB::Block block;
    size_t hostname_idx = 0;
    size_t request_event_time_idx = 0;
    size_t thread_id_idx = 0;
    size_t session_id_idx = 0;
    size_t xid_idx = 0;
    size_t has_watch_idx = 0;
    size_t op_num_idx = 0;
    size_t path_idx = 0;
    size_t data_idx = 0;
    size_t is_ephemeral_idx = 0;
    size_t is_sequential_idx = 0;
    size_t response_event_time_idx = 0;
    size_t error_idx = 0;
    size_t requests_size_idx = 0;
    size_t version_idx = 0;
};

struct RequestFromLog
{
    Coordination::ZooKeeperRequestPtr request;
    std::optional<Coordination::Error> expected_result;
    std::vector<std::optional<Coordination::Error>> subrequest_expected_results;
    int64_t session_id = 0;
    size_t executor_id = 0;
    bool has_watch = false;
    DB::DateTime64 request_event_time{};
    DB::DateTime64 response_event_time{};
    std::shared_ptr<Coordination::ZooKeeper> connection;
};

struct ZooKeeperRequestFromLogReader
{
    ZooKeeperRequestFromLogReader(const std::string & input_request_log, DB::ContextPtr context)
    {
        std::optional<DB::FormatSettings> format_settings;

        file_read_buf = std::make_unique<DB::ReadBufferFromFile>(input_request_log);
        auto compression_method = DB::chooseCompressionMethod(input_request_log, "");
        file_read_buf = DB::wrapReadBufferWithCompressionMethod(std::move(file_read_buf), compression_method);

        DB::SingleReadBufferIterator read_buffer_iterator(std::move(file_read_buf));
        auto [columns_description, format] = DB::detectFormatAndReadSchema(format_settings, read_buffer_iterator, context);

        DB::ColumnsWithTypeAndName columns;
        columns.reserve(columns_description.size());

        for (const auto & column_description : columns_description)
            columns.push_back(DB::ColumnWithTypeAndName{column_description.type, column_description.name});

        header_block = std::move(columns);

        file_read_buf
            = DB::wrapReadBufferWithCompressionMethod(std::make_unique<DB::ReadBufferFromFile>(input_request_log), compression_method);

        input_format = DB::FormatFactory::instance().getInput(
            format,
            *file_read_buf,
            header_block,
            context,
            context->getSettingsRef()[DB::Setting::max_block_size],
            format_settings,
            DB::FormatParserSharedResources::singleThreaded(context->getSettingsRef()),
            nullptr,
            /*is_remote_fs*/ false,
            DB::CompressionMethod::None,
            false);

        default_acls = getDefaultACLs();
    }

    std::optional<RequestFromLog> getNextRequest(bool for_multi = false)
    {
        RequestFromLog request_from_log;

        if (!current_block)
        {
            auto chunk = input_format->generate();

            if (chunk.empty())
                return std::nullopt;

            current_block.emplace(header_block.cloneWithColumns(chunk.detachColumns()));
            idx_in_block = 0;
        }

        request_from_log.expected_result = current_block->getError(idx_in_block);
        request_from_log.session_id = current_block->getSessionId(idx_in_block);
        request_from_log.has_watch = current_block->hasWatch(idx_in_block);
        request_from_log.executor_id = current_block->getExecutorId(idx_in_block);
        request_from_log.request_event_time = current_block->getRequestEventTime(idx_in_block);
        request_from_log.response_event_time = current_block->getResponseEventTime(idx_in_block);

        const auto move_row_iterator = [&]
        {
            if (idx_in_block == current_block->rows() - 1)
                current_block.reset();
            else
                ++idx_in_block;
        };

        auto op_num = current_block->getOpNum(idx_in_block);
        switch (op_num)
        {
            case Coordination::OpNum::Create:
            {
                auto create_request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
                create_request->path = current_block->getPath(idx_in_block);
                create_request->data = current_block->getData(idx_in_block);
                create_request->is_ephemeral = current_block->isEphemeral(idx_in_block);
                create_request->is_sequential = current_block->isSequential(idx_in_block);
                request_from_log.request = create_request;
                break;
            }
            case Coordination::OpNum::Set:
            {
                auto set_request = std::make_shared<Coordination::ZooKeeperSetRequest>();
                set_request->path = current_block->getPath(idx_in_block);
                set_request->data = current_block->getData(idx_in_block);
                if (auto version = current_block->getVersion(idx_in_block))
                {
                    /// we just need to make sure that the request with version that need to fail, fail when replaying
                    if (request_from_log.expected_result == Coordination::Error::ZBADVERSION)
                        set_request->version = std::numeric_limits<int32_t>::max();
                }
                request_from_log.request = set_request;
                break;
            }
            case Coordination::OpNum::Remove:
            {
                auto remove_request = std::make_shared<Coordination::ZooKeeperRemoveRequest>();
                remove_request->path = current_block->getPath(idx_in_block);
                if (auto version = current_block->getVersion(idx_in_block))
                {
                    /// we just need to make sure that the request with version that need to fail, fail when replaying
                    if (request_from_log.expected_result == Coordination::Error::ZBADVERSION)
                        remove_request->version = std::numeric_limits<int32_t>::max();
                }
                request_from_log.request = remove_request;
                break;
            }
            case Coordination::OpNum::Check:
            case Coordination::OpNum::CheckNotExists:
            {
                auto check_request = std::make_shared<Coordination::ZooKeeperCheckRequest>();
                check_request->path = current_block->getPath(idx_in_block);
                if (auto version = current_block->getVersion(idx_in_block))
                {
                    /// we just need to make sure that the request with version that need to fail, fail when replaying
                    if (request_from_log.expected_result == Coordination::Error::ZBADVERSION)
                        check_request->version = std::numeric_limits<int32_t>::max();
                }
                if (op_num == Coordination::OpNum::CheckNotExists)
                    check_request->not_exists = true;
                request_from_log.request = check_request;
                break;
            }
            case Coordination::OpNum::Sync:
            {
                auto sync_request = std::make_shared<Coordination::ZooKeeperSyncRequest>();
                sync_request->path = current_block->getPath(idx_in_block);
                request_from_log.request = sync_request;
                break;
            }
            case Coordination::OpNum::Get:
            {
                auto get_request = std::make_shared<Coordination::ZooKeeperGetRequest>();
                get_request->path = current_block->getPath(idx_in_block);
                request_from_log.request = get_request;
                break;
            }
            case Coordination::OpNum::SimpleList:
            case Coordination::OpNum::FilteredList:
            {
                auto list_request = std::make_shared<Coordination::ZooKeeperSimpleListRequest>();
                list_request->path = current_block->getPath(idx_in_block);
                request_from_log.request = list_request;
                break;
            }
            case Coordination::OpNum::Exists:
            {
                auto exists_request = std::make_shared<Coordination::ZooKeeperExistsRequest>();
                exists_request->path = current_block->getPath(idx_in_block);
                request_from_log.request = exists_request;
                break;
            }
            case Coordination::OpNum::Multi:
            case Coordination::OpNum::MultiRead:
            {
                if (for_multi)
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Nested multi requests are not allowed");

                auto requests_size = current_block->getRequestsSize(idx_in_block);

                Coordination::Requests requests;
                requests.reserve(requests_size);
                move_row_iterator();

                for (size_t i = 0; i < requests_size; ++i)
                {
                    auto subrequest_from_log = getNextRequest(/*for_multi=*/true);
                    if (!subrequest_from_log)
                        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Failed to fetch subrequest for {}, subrequest index {}", op_num, i);

                    if (!subrequest_from_log->expected_result && request_from_log.expected_result
                        && request_from_log.expected_result == Coordination::Error::ZOK)
                    {
                        subrequest_from_log->expected_result = Coordination::Error::ZOK;
                    }

                    requests.push_back(std::move(subrequest_from_log->request));

                    if (subrequest_from_log->session_id != request_from_log.session_id)
                        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Session id mismatch for subrequest in {}, subrequest index {}", op_num, i);

                    if (subrequest_from_log->executor_id != request_from_log.executor_id)
                        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Executor id mismatch for subrequest in {}, subrequest index {}", op_num, i);

                    request_from_log.subrequest_expected_results.push_back(subrequest_from_log->expected_result);
                }

                request_from_log.request = std::make_shared<Coordination::ZooKeeperMultiRequest>(requests, default_acls);

                return request_from_log;
            }
            default:
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported operation {} ({})", op_num, static_cast<int64_t>(op_num));
        }

        move_row_iterator();

        return request_from_log;
    }

private:
    DB::Block header_block;

    std::unique_ptr<DB::ReadBuffer> file_read_buf;
    DB::InputFormatPtr input_format;

    std::optional<ZooKeeperRequestBlock> current_block;
    size_t idx_in_block = 0;

    Coordination::ACLs default_acls;
};


struct RequestFromLogStats
{
    struct Stats
    {
        std::atomic<size_t> total = 0;
        std::atomic<size_t> unexpected_results = 0;
    };

    Stats write_requests;
    Stats read_requests;
};

struct SetupNodeCollector
{
    explicit SetupNodeCollector(const std::string & setup_nodes_snapshot_path)
    {
        if (setup_nodes_snapshot_path.empty())
            return;

        keeper_context = std::make_shared<DB::KeeperContext>(true, std::make_shared<Coordination::CoordinationSettings>());
        keeper_context->setDigestEnabled(true);
        keeper_context->setSnapshotDisk(
            std::make_shared<DB::DiskLocal>("Keeper-snapshots", setup_nodes_snapshot_path));

        snapshot_manager.emplace(1, keeper_context);
        initial_storage = Coordination::KeeperStorage::create(
            /* tick_time_ms */ 500, /* superdigest */ "", keeper_context, /* initialize_system_nodes */ false);
        auto buffer = snapshot_manager->deserializeLatestSnapshotBufferFromDisk();
        if (buffer)
        {
            snapshot_manager->deserializeSnapshotFromBuffer(buffer, *initial_storage);
            std::cerr << "Loaded initial nodes from snapshot" << std::endl;
        }
        else
        {
            std::cerr << "No initial snapshot found" << std::endl;
            initial_storage->initializeSystemNodes();
        }
    }

    void processRequest(const RequestFromLog & request_from_log)
    {
        if (!request_from_log.expected_result.has_value())
            return;


        auto process_request = [&](const Coordination::ZooKeeperRequest & request, const auto expected_result)
        {
            const auto & path = request.getPath();

            if (nodes_created_during_replay.contains(path))
                return;

            auto op_num = request.getOpNum();

            if (op_num == Coordination::OpNum::Create)
            {
                if (expected_result == Coordination::Error::ZNODEEXISTS)
                {
                    addExpectedNode(path);
                }
                else if (expected_result == Coordination::Error::ZOK)
                {
                    nodes_created_during_replay.insert(path);
                    /// we need to make sure ancestors exist
                    auto position = path.find_last_of('/');
                    if (position != 0)
                    {
                        auto parent_path = path.substr(0, position);
                        addExpectedNode(parent_path);
                    }
                }
            }
            else if (op_num == Coordination::OpNum::Remove)
            {
                if (expected_result == Coordination::Error::ZOK || expected_result == Coordination::Error::ZBADVERSION)
                    addExpectedNode(path);
            }
            else if (op_num == Coordination::OpNum::Set)
            {
                if (expected_result == Coordination::Error::ZOK || expected_result == Coordination::Error::ZBADVERSION)
                    addExpectedNode(path);
            }
            else if (op_num == Coordination::OpNum::Check)
            {
                if (expected_result == Coordination::Error::ZOK || expected_result == Coordination::Error::ZBADVERSION)
                    addExpectedNode(path);
            }
            else if (op_num == Coordination::OpNum::CheckNotExists)
            {
                if (expected_result == Coordination::Error::ZNODEEXISTS || expected_result == Coordination::Error::ZBADVERSION)
                    addExpectedNode(path);
            }
            else if (request.isReadRequest())
            {
                if (expected_result == Coordination::Error::ZOK)
                    addExpectedNode(path);
            }
        };

        const auto & request = request_from_log.request;
        if (request->getOpNum() == Coordination::OpNum::Multi || request->getOpNum() == Coordination::OpNum::MultiRead)
        {
            const auto & multi_request = dynamic_cast<const Coordination::ZooKeeperMultiRequest &>(*request);
            const auto & subrequests = multi_request.requests;

            for (size_t i = 0; i < subrequests.size(); ++i)
            {
                const auto & zookeeper_request = dynamic_cast<const Coordination::ZooKeeperRequest &>(*subrequests[i]);
                const auto subrequest_expected_result = request_from_log.subrequest_expected_results[i];
                if (subrequest_expected_result.has_value())
                    process_request(zookeeper_request, *subrequest_expected_result);

            }
        }
        else
            process_request(*request, *request_from_log.expected_result);
    }

    void addExpectedNode(const std::string & path)
    {
        std::lock_guard lock(nodes_mutex);

        if (initial_storage->nodes_storage->getCommittedNodeSimple(path, /*out_stats=*/nullptr, /*out_data=*/nullptr))
            return;

        new_nodes = true;
        std::cerr << "Adding expected node " << path << std::endl;

        Coordination::Requests create_ops;

        size_t pos = 1;
        while (true)
        {
            pos = path.find('/', pos);
            if (pos == std::string::npos)
                break;

            auto request = zkutil::makeCreateRequest(path.substr(0, pos), "", zkutil::CreateMode::Persistent, true);
            create_ops.emplace_back(request);
            ++pos;
        }

        auto request = zkutil::makeCreateRequest(path, "", zkutil::CreateMode::Persistent, true);
        create_ops.emplace_back(request);

        auto next_zxid = initial_storage->getNextZXID();

        static Coordination::ACLs default_acls = getDefaultACLs();

        auto multi_create_request = std::make_shared<Coordination::ZooKeeperMultiRequest>(create_ops, default_acls);
        initial_storage->preprocessRequest(multi_create_request, 1, 0, next_zxid, /* check_acl = */ false, /*digest=*/std::nullopt, /*log_idx=*/0);
        auto responses = initial_storage->processRequest(multi_create_request, 1, next_zxid);
        if (responses.size() > 1 || responses[0].response->error != Coordination::Error::ZOK)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid response after trying to create a node {}", responses[0].response->error);
    }

    void generateSnapshot()
    {
        std::lock_guard lock(nodes_mutex);
        if (!new_nodes)
        {
            std::cerr << "No new nodes added" << std::endl;
            return;
        }

        std::cerr << "Generating snapshot with starting data" << std::endl;
        DB::SnapshotMetadataPtr snapshot_meta = std::make_shared<DB::SnapshotMetadata>(initial_storage->getZXID(), 1, std::make_shared<nuraft::cluster_config>());
        DB::KeeperStorageSnapshot snapshot(initial_storage.get(), snapshot_meta, nullptr, keeper_context->getWriteSnapshotVersion());
        snapshot_manager->serializeSnapshotToDisk(snapshot);

        new_nodes = false;
    }

    std::mutex nodes_mutex;
    DB::KeeperContextPtr keeper_context;
    std::shared_ptr<Coordination::KeeperStorage> initial_storage;
    std::unordered_set<std::string> nodes_created_during_replay;
    std::optional<Coordination::KeeperSnapshotManager> snapshot_manager;
    bool new_nodes = false;
};

void dumpStats(std::string_view type, const RequestFromLogStats::Stats & stats_for_type)
{
    std::cerr << fmt::format(
        "{} requests: {} total, {} with unexpected results ({:.4}%)",
        type,
        stats_for_type.total.load(),
        stats_for_type.unexpected_results.load(),
        stats_for_type.total != 0 ? static_cast<double>(stats_for_type.unexpected_results) / static_cast<double>(stats_for_type.total) * 100 : 0.0)
              << std::endl;
};

void requestFromLogExecutor(
    std::shared_ptr<ConcurrentBoundedQueue<RequestFromLog>> queue,
    RequestFromLogStats & request_stats,
    Stats * bench_info)
{
    RequestFromLog request_from_log;
    std::optional<std::future<void>> last_request;
    while (queue->pop(request_from_log))
    {
        auto request_promise = std::make_shared<std::promise<void>>();
        last_request = request_promise->get_future();
        auto start_time = std::chrono::steady_clock::now();
        Coordination::ResponseCallback callback = [&,
                                                  request_promise,
                                                  start_time,
                                                  request = request_from_log.request,
                                                  expected_result = request_from_log.expected_result,
                                                  subrequest_expected_results = std::move(request_from_log.subrequest_expected_results),
                                                  bench_info](
                                                     const Coordination::Response & response) mutable
        {
            auto & stats = request->isReadRequest() ? request_stats.read_requests : request_stats.write_requests;

            stats.total.fetch_add(1, std::memory_order_relaxed);

            if (bench_info)
            {
                auto end_time = std::chrono::steady_clock::now();
                auto microseconds = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count());
                size_t response_bytes = response.bytesSize();
                if (request->isReadRequest())
                    bench_info->addRead(microseconds, 1, request->bytesSize() + response_bytes);
                else
                    bench_info->addWrite(microseconds, 1, request->bytesSize() + response_bytes);
            }

            if (expected_result)
            {
                if (*expected_result != response.error)
                    stats.unexpected_results.fetch_add(1, std::memory_order_relaxed);

                if (bench_info && *expected_result != response.error)
                    bench_info->errors.fetch_add(1, std::memory_order_relaxed);

#if 0
                if (*expected_result != response.error)
                {
                    std::cerr << fmt::format(
                        "Unexpected result for {}\ngot {}, expected {}\n", request->toString(), response.error, *expected_result)
                              << std::endl;

                    if (const auto * multi_response = dynamic_cast<const Coordination::ZooKeeperMultiResponse *>(&response))
                    {
                        std::string subresponses;
                        for (size_t i = 0; i < multi_response->responses.size(); ++i)
                        {
                            subresponses += fmt::format("{} = {}\n", i, multi_response->responses[i]->error);
                        }

                        std::cerr << "Subresponses\n" << subresponses << std::endl;
                    }
                }
#endif
            }

            request_promise->set_value();
        };

        Coordination::WatchCallbackPtr watch;
        if (request_from_log.has_watch)
            watch = std::make_shared<Coordination::WatchCallback>([](const Coordination::WatchResponse &) {});

        request_from_log.connection->executeGenericRequest(request_from_log.request, callback, watch);
    }

    if (last_request)
        last_request->wait();
}

}

LogRunner::LogRunner(
        std::optional<size_t> concurrency_,
        const std::string & config_path,
        const std::string & input_request_log_,
        const std::string & setup_nodes_snapshot_path_,
        const Strings & hosts_strings_,
        std::optional<double> delay_)
        : input_request_log(input_request_log_)
        , setup_nodes_snapshot_path(setup_nodes_snapshot_path_)
        , info(std::make_shared<Stats>())
{
    if (input_request_log.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "--input-request-log cannot be empty in replay mode");

    if (!std::filesystem::exists(input_request_log))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "File on path {} does not exist", input_request_log);

    if (!config_path.empty())
    {
        DB::ConfigProcessor config_processor(config_path, true, false);
        config_ptr = config_processor.loadConfig().configuration;
    }

    std::cerr << "---- Run options ---- " << std::endl;
    std::cerr << "Replaying requests from: " << input_request_log << std::endl;

    concurrency = getOption<size_t>(concurrency_, config_ptr, "concurrency", 1);
    std::cerr << "Concurrency: " << concurrency << std::endl;

    delay = getOption<double>(delay_, config_ptr, "report_delay", 1.0);
    std::cerr << "Report delay: " << delay << std::endl;

    if (config_ptr)
    {
        nodes_setup.initializeFromConfig(*config_ptr);
        output.initializeFromConfig(*config_ptr);
    }

    /// In setup-nodes-collection mode requests are not sent anywhere, so no connections are needed.
    if (setup_nodes_snapshot_path.empty())
    {
        bool enable_tracing = config_ptr ? config_ptr->getBool("enable_tracing", false) : false;
        connection_factory.initialize(hosts_strings_, config_ptr.get(), enable_tracing);
    }
    else
        std::cerr << "Collecting setup nodes into: " << setup_nodes_snapshot_path << std::endl;

    std::cerr << "---- Run options ----\n" << std::endl;
}

void LogRunner::runBenchmark()
{
    shared_context = DB::Context::createShared();
    global_context = DB::Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    DB::registerFormats();

    if (!setup_nodes_snapshot_path.empty())
        collectSetupNodes();
    else
        replay();
}

void LogRunner::collectSetupNodes()
{
    std::cerr << fmt::format("Collecting setup nodes from {}", input_request_log) << std::endl;

    SetupNodeCollector collector(setup_nodes_snapshot_path);

    ZooKeeperRequestFromLogReader request_reader(input_request_log, global_context);

    DB::InterruptListener interrupt_listener;
    delay_watch.restart();
    while (auto request_from_log = request_reader.getNextRequest())
    {
        collector.processRequest(*request_from_log);

        if (interrupt_listener.check())
        {
            std::cerr << "Stopping. SIGINT received." << std::endl;
            break;
        }

        if (delay > 0 && delay_watch.elapsedSeconds() > delay)
        {
            collector.generateSnapshot();
            delay_watch.restart();
        }
    }

    collector.generateSnapshot();
}

void LogRunner::replay()
{
    std::cerr << fmt::format("Running benchmark using requests from {}", input_request_log) << std::endl;

    pool.emplace(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, concurrency);

    /// Randomly choosing connection index
    pcg64 rng(randomSeed());
    const auto & connection_infos = connection_factory.connectionInfos();
    std::uniform_int_distribution<size_t> connection_distribution(0, connection_infos.size() - 1);

    std::unordered_map<int64_t, std::shared_ptr<Coordination::ZooKeeper>> zookeeper_connections;
    auto get_zookeeper_connection = [&](int64_t session_id)
    {
        if (auto it = zookeeper_connections.find(session_id); it != zookeeper_connections.end() && !it->second->isExpired())
            return it->second;

        auto connection_idx = connection_distribution(rng);
        auto zk_connection = connection_factory.getConnection(connection_infos[connection_idx], connection_idx);
        zookeeper_connections.insert_or_assign(session_id, zk_connection);
        return zk_connection;
    };

    RequestFromLogStats stats;

    std::unordered_map<uint64_t, std::shared_ptr<ConcurrentBoundedQueue<RequestFromLog>>> executor_id_to_queue;

    SCOPE_EXIT_SAFE({
        for (const auto & [executor_id, executor_queue] : executor_id_to_queue)
            executor_queue->finish();

        pool->wait();

        dumpStats("Write", stats.write_requests);
        dumpStats("Read", stats.read_requests);
        info->report(*info);
        DB::WriteBufferFromOwnString out;
        info->writeJSON(out, 0);
        output.write(out.str(), 0);
    });

    auto push_request = [&](RequestFromLog request)
    {
        if (auto it = executor_id_to_queue.find(request.executor_id); it != executor_id_to_queue.end())
        {
            auto success = it->second->push(std::move(request));
            if (!success)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Failed to push to the executor's queue");
            return;
        }

        auto executor_queue = std::make_shared<ConcurrentBoundedQueue<RequestFromLog>>(std::numeric_limits<uint64_t>::max());
        executor_id_to_queue.emplace(request.executor_id, executor_queue);
        auto scheduled = pool->trySchedule([&, executor_queue]() mutable
        {
            requestFromLogExecutor(std::move(executor_queue), stats, info.get());
        });

        if (!scheduled)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Failed to schedule worker, try to increase concurrency parameter");

        auto success = executor_queue->push(std::move(request));
        if (!success)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Failed to push to the executor's queue");
    };

    auto setup_connection = connection_factory.getConnection(connection_infos[0], 0);
    nodes_setup.startup(*setup_connection);

    ZooKeeperRequestFromLogReader request_reader(input_request_log, global_context);

    DB::InterruptListener interrupt_listener;
    delay_watch.restart();
    while (auto request_from_log = request_reader.getNextRequest())
    {
        request_from_log->connection = get_zookeeper_connection(request_from_log->session_id);
        request_from_log->executor_id %= concurrency;
        push_request(std::move(*request_from_log));

        if (interrupt_listener.check())
        {
            std::cerr << "Stopping. SIGINT received." << std::endl;
            break;
        }

        if (delay > 0 && delay_watch.elapsedSeconds() > delay)
        {
            dumpStats("Write", stats.write_requests);
            dumpStats("Read", stats.read_requests);
            std::cerr << std::endl;
            delay_watch.restart();
        }
    }
}

LogRunner::~LogRunner()
{
    if (pool)
        pool->wait();

    /// In setup-nodes-collection mode nothing was created on a server.
    if (!setup_nodes_snapshot_path.empty() || !nodes_setup.hasNodes())
        return;

    try
    {
        auto connection = connection_factory.getConnection(connection_factory.connectionInfos()[0], 0);
        nodes_setup.cleanup(*connection);
    }
    catch (...)
    {
        DB::tryLogCurrentException("While trying to clean nodes");
    }
}
