#include <Coordination/KeeperDispatcher.h>
#include <Poco/Path.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Common/hex.h>
#include <Common/setThreadName.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/checkStackSize.h>
#include <Common/CurrentMetrics.h>

#if USE_AWS_S3
#include <Core/UUID.h>

#include <IO/S3Common.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#endif

#include <future>
#include <chrono>
#include <filesystem>
#include <iterator>
#include <limits>

namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
    extern const Metric KeeperOutstandingRequets;
}

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int SYSTEM_ERROR;
}


KeeperDispatcher::KeeperDispatcher()
    : responses_queue(std::numeric_limits<size_t>::max())
    , read_requests_queue(std::numeric_limits<size_t>::max())
    , finalize_requests_queue(std::numeric_limits<size_t>::max())
#if USE_AWS_S3
    , snapshots_s3_queue(std::numeric_limits<size_t>::max())
#endif
    , configuration_and_settings(std::make_shared<KeeperConfigurationAndSettings>())
    , log(&Poco::Logger::get("KeeperDispatcher"))
{
}


/// ZooKeepers has 2 requirements:
/// - writes need to be linearizable
/// - all requests from single session need to be processed in the order of their arrival
///
/// Because of that, we cannot process read and write requests from SAME session at the same time.
/// To be able to process read and write requests in parallel we need to make sure that only 1 type
/// of request is being processed from a single session.
/// Multiple types from different sessions can be processed at the same time.
///
/// We do some in-session housekeeping to make sure that the multithreaded request processing is correct.
/// When a request is received from a client, we check if there are requests being processed from that same
/// session, and if yes, of what type. If the types are the same, and there are no requests of different
/// type inbetetween, we can instanly add it to active request queue. Otherwise, we need to wait until
/// all requests of the other type are processed.
///
/// There are multiple threads used for processing the request, each of them communicating with a queue.
/// Assumption: only one type of request is being processed from a same session at any point in time (read or write).
///
/// requestThread -> requests currently being processed
/// readRequestThread -> thread for processing read requests
/// finalizeRequestThread -> thread for finalizing requests:
///                          - in-session housekeeping, add requests to the active request queue if there are any
///
/// If reads are linearizable without quorum, a request can possibly wait for a certain log to be committed.
/// In that case we add it to the waiting queue for that log.
/// When that log is committed, the committing thread will send that read request to readRequestThread so it can be processed.
///
void KeeperDispatcher::requestThread()
{
    setThreadName("KeeperReqT");

    /// Result of requests batch from previous iteration
    RaftResult prev_result = nullptr;
    const auto previous_quorum_done = [&] { return !prev_result || prev_result->has_result() || prev_result->get_result_code() != nuraft::cmd_result_code::OK; };

    const auto needs_quorum = [](const auto & coordination_settings, const auto & request)
    {
        return coordination_settings->quorum_reads || coordination_settings->read_mode.toString() == "quorum" || !request.request->isReadRequest();
    };

    KeeperStorage::RequestsForSessions quorum_requests;
    KeeperStorage::RequestsForSessions read_requests;

    auto process_quorum_requests = [&, this]() mutable
    {
        /// Forcefully process all previous pending requests
        if (prev_result)
            forceWaitAndProcessResult(prev_result);

        prev_result = server->putRequestBatch(quorum_requests);

        if (prev_result)
        {
            prev_result->when_ready([&, requests_for_sessions = std::move(quorum_requests)](nuraft::cmd_result<nuraft::ptr<nuraft::buffer>> & result, nuraft::ptr<std::exception> &) mutable
            {
                if (!result.get_accepted() || result.get_result_code() == nuraft::cmd_result_code::TIMEOUT)
                    addErrorResponses(requests_for_sessions, Coordination::Error::ZOPERATIONTIMEOUT);
                else if (result.get_result_code() != nuraft::cmd_result_code::OK)
                    addErrorResponses(requests_for_sessions, Coordination::Error::ZCONNECTIONLOSS);
            });
        }

        quorum_requests.clear();
    };

    /// ZooKeeper requires that the requests inside a single session are processed in a strict order
    /// (we cannot process later requests before all the previous once are processed)
    /// By making sure that at this point we can either have just read requests or just write requests
    /// from a single session, we can process them independently
    while (!shutdown_called)
    {
        KeeperStorage::RequestForSession request;

        auto coordination_settings = configuration_and_settings->coordination_settings;
        uint64_t max_wait = coordination_settings->operation_timeout_ms.totalMilliseconds();
        uint64_t max_batch_size = coordination_settings->max_requests_batch_size;

        try
        {
            if (active_requests_queue->tryPop(request, max_wait))
            {
                CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequets);
                if (shutdown_called)
                    break;

                if (needs_quorum(coordination_settings, request))
                    quorum_requests.emplace_back(request);
                else
                    read_requests.emplace_back(request);

                /// Waiting until previous append will be successful, or batch is big enough
                /// has_result == false && get_result_code == OK means that our request still not processed.
                /// Sometimes NuRaft set errorcode without setting result, so we check both here.
                while (true)
                {
                    if (quorum_requests.size() > max_batch_size)
                        break;

                    if (read_requests.size() > max_batch_size)
                    {
                        processReadRequests(coordination_settings, read_requests);

                        if (previous_quorum_done())
                            break;
                    }

                    /// Trying to get batch requests as fast as possible
                    if (active_requests_queue->tryPop(request, 1))
                    {
                        CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequets);
                        if (needs_quorum(coordination_settings, request))
                            quorum_requests.emplace_back(request);
                        else
                            read_requests.emplace_back(request);
                    }
                    else
                    {
                        /// batch of read requests can send at most one request
                        /// so we don't care if the previous batch hasn't received response
                        if (!read_requests.empty())
                            processReadRequests(coordination_settings, read_requests);

                        /// if we still didn't process previous batch we can
                        /// increase are current batch even more
                        if (previous_quorum_done())
                            break;
                    }

                    if (shutdown_called)
                        break;
                }

                if (shutdown_called)
                    break;

                if (!quorum_requests.empty())
                    process_quorum_requests();

            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void KeeperDispatcher::processReadRequests(const CoordinationSettingsPtr & coordination_settings, KeeperStorage::RequestsForSessions & read_requests)
{
    if (coordination_settings->read_mode.toString() == "fastlinear")
    {
        // we just want to know what's the current latest committed log on Leader node
        auto leader_info_result = server->getLeaderInfo();
        if (leader_info_result)
        {
            leader_info_result->when_ready([&, requests_for_sessions = std::move(read_requests)](nuraft::cmd_result<nuraft::ptr<nuraft::buffer>> & result, nuraft::ptr<std::exception> & exception) mutable
            {
                if (!result.get_accepted() || result.get_result_code() == nuraft::cmd_result_code::TIMEOUT)
                {
                    addErrorResponses(requests_for_sessions, Coordination::Error::ZOPERATIONTIMEOUT);
                    return;
                }

                if (result.get_result_code() != nuraft::cmd_result_code::OK)
                {
                    addErrorResponses(requests_for_sessions, Coordination::Error::ZCONNECTIONLOSS);
                    return;
                }

                if (exception)
                {
                    LOG_INFO(log, "Got exception while waiting for read results {}", exception->what());
                    addErrorResponses(requests_for_sessions, Coordination::Error::ZCONNECTIONLOSS);
                    return;
                }

                auto & leader_info_ctx = result.get();

                if (!leader_info_ctx)
                {
                    addErrorResponses(requests_for_sessions, Coordination::Error::ZCONNECTIONLOSS);
                    return;
                }

                KeeperServer::NodeInfo leader_info;
                leader_info.term = leader_info_ctx->get_ulong();
                leader_info.last_committed_index = leader_info_ctx->get_ulong();
                std::lock_guard lock(leader_waiter_mutex);
                auto node_info = server->getNodeInfo();

                /// we're behind, we need to wait
                if (node_info.term < leader_info.term || node_info.last_committed_index < leader_info.last_committed_index)
                {
                    auto & leader_waiter = leader_waiters[leader_info];
                    leader_waiter.insert(leader_waiter.end(), requests_for_sessions.begin(), requests_for_sessions.end());
                    LOG_TRACE(log, "waiting for term {}, idx {}", leader_info.term, leader_info.last_committed_index);
                }
                /// process it in background thread
                else if (!read_requests_queue.push(std::move(requests_for_sessions)))
                    throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push read requests to queue");
            });
        }
    }
    else
    {
        assert(coordination_settings->read_mode.toString() == "nonlinear");
        if (!read_requests_queue.push(std::move(read_requests)))
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push read requests to queue");
    }

    read_requests.clear();
}

void KeeperDispatcher::responseThread()
{
    setThreadName("KeeperRspT");
    while (!shutdown_called)
    {
        KeeperStorage::ResponseForSession response_for_session;

        uint64_t max_wait = configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds();

        if (responses_queue.tryPop(response_for_session, max_wait))
        {
            if (shutdown_called)
                break;

            try
            {
                 setResponse(response_for_session.session_id, response_for_session.response);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void KeeperDispatcher::snapshotThread()
{
    setThreadName("KeeperSnpT");
    while (!shutdown_called)
    {
        CreateSnapshotTask task;
        if (!snapshots_queue.pop(task))
            break;

        if (shutdown_called)
            break;

        try
        {
            [[maybe_unused]] auto snapshot_path = task.create_snapshot(std::move(task.snapshot));

#if USE_AWS_S3
            if (snapshot_path.empty())
                continue;

            if (isLeader() && getSnapshotS3Client() != nullptr)
            {
                if (!snapshots_s3_queue.push(snapshot_path))
                    LOG_WARNING(log, "Failed to add snapshot {} to S3 queue", snapshot_path);
            }
#endif
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

#if USE_AWS_S3
struct KeeperDispatcher::S3Configuration
{
    S3Configuration(S3::URI uri_, S3::AuthSettings auth_settings_, std::shared_ptr<const Aws::S3::S3Client> client_)
        : uri(std::move(uri_))
        , auth_settings(std::move(auth_settings_))
        , client(std::move(client_))
    {}

    S3::URI uri;
    S3::AuthSettings auth_settings;
    std::shared_ptr<const Aws::S3::S3Client> client;
};

void KeeperDispatcher::updateS3Configuration(const Poco::Util::AbstractConfiguration & config)
{
    try
    {
        const std::string config_prefix = "keeper_server.s3_snapshot";

        if (!config.has(config_prefix))
        {
            std::lock_guard client_lock{snapshot_s3_client_mutex};
            if (snapshot_s3_client)
                LOG_INFO(log, "S3 configuration was removed");
            snapshot_s3_client = nullptr;
            return;
        }

        auto auth_settings = S3::AuthSettings::loadFromConfig(config_prefix, config);

        auto endpoint = config.getString(config_prefix + ".endpoint");
        auto new_uri = S3::URI{Poco::URI(endpoint)};

        std::unique_lock client_lock{snapshot_s3_client_mutex};

        if (snapshot_s3_client && snapshot_s3_client->client && auth_settings == snapshot_s3_client->auth_settings
            && snapshot_s3_client->uri.uri == new_uri.uri)
            return;

        LOG_INFO(log, "S3 configuration was updated");

        client_lock.unlock();

        auto credentials = Aws::Auth::AWSCredentials(auth_settings.access_key_id, auth_settings.secret_access_key);
        HeaderCollection headers = auth_settings.headers;

        static constexpr size_t s3_max_redirects = 10;
        static constexpr bool enable_s3_requests_logging = false;

        if (!new_uri.key.empty())
        {
            LOG_ERROR(log, "Invalid endpoint defined for S3, it shouldn't contain key, endpoint: {}", endpoint);
            return;
        }

        S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
            auth_settings.region,
            RemoteHostFilter(), s3_max_redirects,
            enable_s3_requests_logging,
            /* for_disk_s3 = */ false);

        client_configuration.endpointOverride = new_uri.endpoint;

        auto client = S3::ClientFactory::instance().create(
            client_configuration,
            new_uri.is_virtual_hosted_style,
            credentials.GetAWSAccessKeyId(),
            credentials.GetAWSSecretKey(),
            auth_settings.server_side_encryption_customer_key_base64,
            std::move(headers),
            auth_settings.use_environment_credentials.value_or(false),
            auth_settings.use_insecure_imds_request.value_or(false));

        auto new_client = std::make_shared<KeeperDispatcher::S3Configuration>(std::move(new_uri), std::move(auth_settings), std::move(client));

        client_lock.lock();
        snapshot_s3_client = std::move(new_client);
        client_lock.unlock();
        LOG_INFO(log, "S3 client was updated");
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to create an S3 client for snapshots");
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void KeeperDispatcher::snapshotS3Thread()
{
    setThreadName("KeeperS3SnpT");

    auto uuid = UUIDHelpers::generateV4();
    while (!shutdown_called)
    {
        std::string snapshot_path;
        if (!snapshots_s3_queue.pop(snapshot_path))
            break;

        if (shutdown_called)
            break;

        try
        {
            auto s3_client = getSnapshotS3Client();
            if (s3_client == nullptr)
                continue;

            LOG_INFO(log, "Will try to upload snapshot on {} to S3", snapshot_path);
            ReadBufferFromFile snapshot_file(snapshot_path);

            S3Settings::ReadWriteSettings read_write_settings;
            read_write_settings.upload_part_size_multiply_parts_count_threshold = 10000;

            const auto create_writer = [&](const auto & key)
            {
                return WriteBufferFromS3
                {
                    s3_client->client,
                    s3_client->uri.bucket,
                    key,
                    read_write_settings
                };
            };

            auto snapshot_name = fs::path(snapshot_path).filename().string();
            auto lock_file = fmt::format(".{}_LOCK", snapshot_name);

            const auto file_exists = [&](const auto & key)
            {
                Aws::S3::Model::HeadObjectRequest request;
                request.SetBucket(s3_client->uri.bucket);
                request.SetKey(key);
                auto outcome = s3_client->client->HeadObject(request);

                if (outcome.IsSuccess())
                    return true;

                const auto & error = outcome.GetError();
                if (error.GetErrorType() != Aws::S3::S3Errors::NO_SUCH_KEY && error.GetErrorType() != Aws::S3::S3Errors::RESOURCE_NOT_FOUND)
                    throw S3Exception(error.GetErrorType(), "Failed to verify existence of lock file: {}", error.GetMessage());

                return false;
            };

            if (file_exists(snapshot_name))
            {
                LOG_ERROR(log, "Snapshot {} already exists", snapshot_name);
                continue;
            }

            // First we need to verify that there isn't already a lock file for the snapshot we want to upload
            if (file_exists(lock_file))
            {
                LOG_ERROR(log, "Lock file for {} already, exists. Probably a different node is already uploading the snapshot", snapshot_name);
                continue;
            }

            // We write our UUID to lock file
            LOG_DEBUG(log, "Trying to create a lock file");
            WriteBufferFromS3 lock_writer = create_writer(lock_file);
            writeUUIDText(uuid, lock_writer);
            lock_writer.finalize();

            const auto read_lock_file = [&]() -> std::string
            {
                ReadBufferFromS3 lock_reader
                {
                    s3_client->client,
                    s3_client->uri.bucket,
                    lock_file,
                    "",
                    1,
                    {}
                };

                std::string read_uuid;
                readStringUntilEOF(read_uuid, lock_reader);

                return read_uuid;
            };

            // We read back the written UUID, if it's the same we can upload the file
            auto read_uuid = read_lock_file();

            if (read_uuid != toString(uuid))
            {
                LOG_ERROR(log, "Failed to create a lock file");
                continue;
            }

            WriteBufferFromS3 snapshot_writer = create_writer(snapshot_name);
            copyData(snapshot_file, snapshot_writer);
            snapshot_writer.finalize();

            LOG_INFO(log, "Successfully uploaded {} to S3", snapshot_path);

            LOG_INFO(log, "Removing lock file");
            Aws::S3::Model::DeleteObjectRequest delete_request;
            delete_request.SetBucket(s3_client->uri.bucket);
            delete_request.SetKey(lock_file);
            auto delete_outcome = s3_client->client->DeleteObject(delete_request);

            if (!delete_outcome.IsSuccess())
                throw S3Exception(delete_outcome.GetError().GetMessage(), delete_outcome.GetError().GetErrorType());

        }
        catch (...)
        {
            LOG_INFO(log, "Failure during upload of {} to S3", snapshot_path);
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}
#endif

/// Background thread for processing read requests
void KeeperDispatcher::readRequestThread()
{
    setThreadName("KeeperReadT");
    while (!shutdown_called)
    {
        KeeperStorage::RequestsForSessions requests;
        if (!read_requests_queue.pop(requests))
            break;

        if (shutdown_called)
            break;

        try
        {
            for (const auto & request_info : requests)
            {
                if (server->isLeaderAlive())
                    server->putLocalReadRequest(request_info);
                else
                    addErrorResponses({request_info}, Coordination::Error::ZCONNECTIONLOSS);
            }

            if (!finalize_requests_queue.push(std::move(requests)))
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push read requests to queue");
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

/// We finalize requests every time we commit a single log with request
/// or process a batch of read requests.
/// Because it can get heavy, we do it in background thread.
void KeeperDispatcher::finalizeRequestsThread()
{
    setThreadName("KeeperFinalT");
    while (!shutdown_called)
    {
        KeeperStorage::RequestsForSessions requests;
        if (!finalize_requests_queue.pop(requests))
            break;

        if (shutdown_called)
            break;

        try
        {
            finalizeRequests(requests);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void KeeperDispatcher::setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response)
{
    std::lock_guard lock(session_to_response_callback_mutex);

    /// Special new session response.
    if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::SessionID)
    {
        const Coordination::ZooKeeperSessionIDResponse & session_id_resp = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response);

        /// Nobody waits for this session id
        if (session_id_resp.server_id != server->getServerID() || !new_session_id_response_callback.contains(session_id_resp.internal_id))
            return;

        auto callback = new_session_id_response_callback[session_id_resp.internal_id];
        callback(response);
        new_session_id_response_callback.erase(session_id_resp.internal_id);
    }
    else /// Normal response, just write to client
    {
        auto session_response_callback = session_to_response_callback.find(session_id);

        /// Session was disconnected, just skip this response
        if (session_response_callback == session_to_response_callback.end())
        {
            LOG_TEST(log, "Cannot write response xid={}, op={}, session {} disconnected",
                response->xid, response->xid == Coordination::WATCH_XID ? "Watch" : toString(response->getOpNum()), session_id);
            return;
        }

        session_response_callback->second(response);

        /// Session closed, no more writes
        if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::Close)
        {
            session_to_response_callback.erase(session_response_callback);
            CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);
        }
    }
}

bool KeeperDispatcher::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
{
    {
        /// If session was already disconnected than we will ignore requests
        std::lock_guard lock(session_to_response_callback_mutex);
        if (!session_to_response_callback.contains(session_id))
            return false;
    }

    KeeperStorage::RequestForSession request_info;
    request_info.request = request;
    using namespace std::chrono;
    request_info.time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    request_info.session_id = session_id;

    {
        std::lock_guard lock{unprocessed_request_mutex};
        auto unprocessed_requests_it = unprocessed_requests_for_session.find(session_id);
        if (unprocessed_requests_it == unprocessed_requests_for_session.end())
        {
            auto & unprocessed_requests = unprocessed_requests_for_session[session_id];
            unprocessed_requests.unprocessed_num = 1;
            unprocessed_requests.is_read = request->isReadRequest();
        }
        else
        {
            auto & unprocessed_requests = unprocessed_requests_it->second;

            /// queue is not empty, or the request types don't match, put it in the waiting queue
            if (!unprocessed_requests.request_queue.empty() || unprocessed_requests.is_read != request->isReadRequest())
            {
                unprocessed_requests.request_queue.push_back(std::move(request_info));
                return true;
            }

            ++unprocessed_requests.unprocessed_num;
        }
    }

    std::lock_guard lock(push_request_mutex);

    if (shutdown_called)
        return false;

    /// Put close requests without timeouts
    if (request->getOpNum() == Coordination::OpNum::Close)
    {
        if (!active_requests_queue->push(std::move(request_info)))
            throw Exception("Cannot push request to queue", ErrorCodes::SYSTEM_ERROR);
    }
    else if (!active_requests_queue->tryPush(std::move(request_info), configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds()))
    {
        throw Exception("Cannot push request to queue within operation timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    }
    CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequets);
    return true;
}

void KeeperDispatcher::initialize(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper, bool start_async)
{
    LOG_DEBUG(log, "Initializing storage dispatcher");

    configuration_and_settings = KeeperConfigurationAndSettings::loadFromConfig(config, standalone_keeper);
    active_requests_queue = std::make_unique<RequestsQueue>(configuration_and_settings->coordination_settings->max_requests_batch_size);

    request_thread = ThreadFromGlobalPool([this] { requestThread(); });
    responses_thread = ThreadFromGlobalPool([this] { responseThread(); });
    snapshot_thread = ThreadFromGlobalPool([this] { snapshotThread(); });
    read_request_thread = ThreadFromGlobalPool([this] { readRequestThread(); });
    finalize_requests_thread = ThreadFromGlobalPool([this] { finalizeRequestsThread(); });

#if USE_AWS_S3
    snapshot_s3_thread = ThreadFromGlobalPool([this] { snapshotS3Thread(); });
#endif

    server = std::make_unique<KeeperServer>(
        configuration_and_settings,
        config,
        responses_queue,
        snapshots_queue,
        [this](const KeeperStorage::RequestForSession & request_for_session, uint64_t log_term, uint64_t log_idx)
        { onRequestCommit(request_for_session, log_term, log_idx); },
        [this](uint64_t term, uint64_t last_idx)
        { onApplySnapshot(term, last_idx); });

    try
    {
        LOG_DEBUG(log, "Waiting server to initialize");
        server->startup(config, configuration_and_settings->enable_ipv6);
        LOG_DEBUG(log, "Server initialized, waiting for quorum");

        if (!start_async)
        {
            server->waitInit();
            LOG_DEBUG(log, "Quorum initialized");
        }
        else
        {
            LOG_INFO(log, "Starting Keeper asynchronously, server will accept connections to Keeper when it will be ready");
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    /// Start it after keeper server start
    session_cleaner_thread = ThreadFromGlobalPool([this] { sessionCleanerTask(); });
    update_configuration_thread = ThreadFromGlobalPool([this] { updateConfigurationThread(); });
    updateConfiguration(config);

    LOG_DEBUG(log, "Dispatcher initialized");
}

void KeeperDispatcher::shutdown()
{
    try
    {
        {
            std::lock_guard lock(push_request_mutex);

            if (shutdown_called)
                return;

            LOG_DEBUG(log, "Shutting down storage dispatcher");
            shutdown_called = true;

            if (session_cleaner_thread.joinable())
                session_cleaner_thread.join();

            if (active_requests_queue)
            {
                active_requests_queue->finish();

                if (request_thread.joinable())
                    request_thread.join();
            }

            responses_queue.finish();
            if (responses_thread.joinable())
                responses_thread.join();

            snapshots_queue.finish();
            if (snapshot_thread.joinable())
                snapshot_thread.join();

#if USE_AWS_S3
            snapshots_s3_queue.finish();
            if (snapshot_s3_thread.joinable())
                snapshot_s3_thread.join();
#endif

            read_requests_queue.finish();
            if (read_request_thread.joinable())
                read_request_thread.join();

            finalize_requests_queue.finish();
            if (finalize_requests_thread.joinable())
                finalize_requests_thread.join();

            update_configuration_queue.finish();
            if (update_configuration_thread.joinable())
                update_configuration_thread.join();
        }

        KeeperStorage::RequestForSession request_for_session;

        /// Set session expired for all pending requests
        while (active_requests_queue && active_requests_queue->tryPop(request_for_session))
        {
            CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequets);
            auto response = request_for_session.request->makeResponse();
            response->error = Coordination::Error::ZSESSIONEXPIRED;
            setResponse(request_for_session.session_id, response);
        }

        KeeperStorage::RequestsForSessions close_requests;
        {
            /// Clear all registered sessions
            std::lock_guard lock(session_to_response_callback_mutex);

            if (hasLeader())
            {
                close_requests.reserve(session_to_response_callback.size());
                // send to leader CLOSE requests for active sessions
                for (const auto & [session, response] : session_to_response_callback)
                {
                    auto request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
                    request->xid = Coordination::CLOSE_XID;
                    using namespace std::chrono;
                    KeeperStorage::RequestForSession request_info
                    {
                        .session_id = session,
                        .time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count(),
                        .request = std::move(request),
                    };

                    close_requests.push_back(std::move(request_info));
                }
            }

            session_to_response_callback.clear();
        }

        // if there is no leader, there is no reason to do CLOSE because it's a write request
        if (hasLeader() && !close_requests.empty())
        {
            LOG_INFO(log, "Trying to close {} session(s)", close_requests.size());
            const auto raft_result = server->putRequestBatch(close_requests);
            auto sessions_closing_done_promise = std::make_shared<std::promise<void>>();
            auto sessions_closing_done = sessions_closing_done_promise->get_future();
            raft_result->when_ready([sessions_closing_done_promise = std::move(sessions_closing_done_promise)](
                                        nuraft::cmd_result<nuraft::ptr<nuraft::buffer>> & /*result*/,
                                        nuraft::ptr<std::exception> & /*exception*/) { sessions_closing_done_promise->set_value(); });

            auto session_shutdown_timeout = configuration_and_settings->coordination_settings->session_shutdown_timeout.totalMilliseconds();
            if (sessions_closing_done.wait_for(std::chrono::milliseconds(session_shutdown_timeout)) != std::future_status::ready)
                LOG_WARNING(
                    log,
                    "Failed to close sessions in {}ms. If they are not closed, they will be closed after session timeout.",
                    session_shutdown_timeout);
        }

        if (server)
            server->shutdown();

        CurrentMetrics::set(CurrentMetrics::KeeperAliveConnections, 0);

    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    LOG_DEBUG(log, "Dispatcher shut down");
}

void KeeperDispatcher::forceRecovery()
{
    server->forceRecovery();
}

KeeperDispatcher::~KeeperDispatcher()
{
    shutdown();
}

void KeeperDispatcher::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    if (!session_to_response_callback.try_emplace(session_id, callback).second)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);
    CurrentMetrics::add(CurrentMetrics::KeeperAliveConnections);
}

void KeeperDispatcher::sessionCleanerTask()
{
    while (true)
    {
        if (shutdown_called)
            return;

        try
        {
            /// Only leader node must check dead sessions
            if (server->checkInit() && isLeader())
            {
                auto dead_sessions = server->getDeadSessions();

                for (int64_t dead_session : dead_sessions)
                {
                    LOG_INFO(log, "Found dead session {}, will try to close it", dead_session);

                    /// Close session == send close request to raft server
                    auto request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
                    request->xid = Coordination::CLOSE_XID;
                    using namespace std::chrono;
                    KeeperStorage::RequestForSession request_info
                    {
                        .session_id = dead_session,
                        .time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count(),
                        .request = std::move(request),
                    };
                    {
                        std::lock_guard lock(push_request_mutex);
                        if (!active_requests_queue->push(std::move(request_info)))
                            LOG_INFO(log, "Cannot push close request to queue while cleaning outdated sessions");
                        CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequets);
                    }

                    /// Remove session from registered sessions
                    finishSession(dead_session);
                    LOG_INFO(log, "Dead session close request pushed");
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        auto time_to_sleep = configuration_and_settings->coordination_settings->dead_session_check_period_ms.totalMilliseconds();
        std::this_thread::sleep_for(std::chrono::milliseconds(time_to_sleep));
    }
}

void KeeperDispatcher::finishSession(int64_t session_id)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    auto session_it = session_to_response_callback.find(session_id);
    if (session_it != session_to_response_callback.end())
    {
        session_to_response_callback.erase(session_it);
        CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);
    }
}

void KeeperDispatcher::addErrorResponses(const KeeperStorage::RequestsForSessions & requests_for_sessions, Coordination::Error error)
{
    for (const auto & request_for_session : requests_for_sessions)
    {
        KeeperStorage::ResponsesForSessions responses;
        auto response = request_for_session.request->makeResponse();
        response->xid = request_for_session.request->xid;
        response->zxid = 0;
        response->error = error;
        if (!responses_queue.push(DB::KeeperStorage::ResponseForSession{request_for_session.session_id, response}))
            throw Exception(ErrorCodes::SYSTEM_ERROR,
                "Could not push error response xid {} zxid {} error message {} to responses queue",
                response->xid,
                response->zxid,
                errorMessage(error));
    }
}

void KeeperDispatcher::forceWaitAndProcessResult(RaftResult & result)
{
    if (!result->has_result())
        result->get();

    result = nullptr;
}

#if USE_AWS_S3
std::shared_ptr<KeeperDispatcher::S3Configuration> KeeperDispatcher::getSnapshotS3Client() const
{
    std::lock_guard lock{snapshot_s3_client_mutex};
    return snapshot_s3_client;
}
#endif

int64_t KeeperDispatcher::getSessionID(int64_t session_timeout_ms)
{
    /// New session id allocation is a special request, because we cannot process it in normal
    /// way: get request -> put to raft -> set response for registered callback.
    KeeperStorage::RequestForSession request_info;
    std::shared_ptr<Coordination::ZooKeeperSessionIDRequest> request = std::make_shared<Coordination::ZooKeeperSessionIDRequest>();
    /// Internal session id. It's a temporary number which is unique for each client on this server
    /// but can be same on different servers.
    request->internal_id = internal_session_id_counter.fetch_add(1);
    request->session_timeout_ms = session_timeout_ms;
    request->server_id = server->getServerID();

    request_info.request = request;
    using namespace std::chrono;
    request_info.time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    request_info.session_id = -1;

    auto promise = std::make_shared<std::promise<int64_t>>();
    auto future = promise->get_future();

    {
        std::lock_guard lock(session_to_response_callback_mutex);
        new_session_id_response_callback[request->internal_id] = [promise, internal_id = request->internal_id] (const Coordination::ZooKeeperResponsePtr & response)
        {
            if (response->getOpNum() != Coordination::OpNum::SessionID)
                promise->set_exception(std::make_exception_ptr(Exception(ErrorCodes::LOGICAL_ERROR,
                            "Incorrect response of type {} instead of SessionID response", Coordination::toString(response->getOpNum()))));

            auto session_id_response = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response);
            if (session_id_response.internal_id != internal_id)
            {
                promise->set_exception(std::make_exception_ptr(Exception(ErrorCodes::LOGICAL_ERROR,
                            "Incorrect response with internal id {} instead of {}", session_id_response.internal_id, internal_id)));
            }

            if (response->error != Coordination::Error::ZOK)
                promise->set_exception(std::make_exception_ptr(zkutil::KeeperException("SessionID request failed with error", response->error)));

            promise->set_value(session_id_response.session_id);
        };
    }

    /// Push new session request to queue
    {
        std::lock_guard lock(push_request_mutex);
        if (!active_requests_queue->tryPush(std::move(request_info), session_timeout_ms))
            throw Exception("Cannot push session id request to queue within session timeout", ErrorCodes::TIMEOUT_EXCEEDED);
        CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequets);
    }

    if (future.wait_for(std::chrono::milliseconds(session_timeout_ms)) != std::future_status::ready)
        throw Exception("Cannot receive session id within session timeout", ErrorCodes::TIMEOUT_EXCEEDED);

    /// Forcefully wait for request execution because we cannot process any other
    /// requests for this client until it get new session id.
    return future.get();
}


void KeeperDispatcher::updateConfigurationThread()
{
    while (true)
    {
        if (shutdown_called)
            return;

        try
        {
            using namespace std::chrono_literals;
            if (!server->checkInit())
            {
                LOG_INFO(log, "Server still not initialized, will not apply configuration until initialization finished");
                std::this_thread::sleep_for(5000ms);
                continue;
            }

            if (server->isRecovering())
            {
                LOG_INFO(log, "Server is recovering, will not apply configuration until recovery is finished");
                std::this_thread::sleep_for(5000ms);
                continue;
            }

            ConfigUpdateAction action;
            if (!update_configuration_queue.pop(action))
                break;


            /// We must wait this update from leader or apply it ourself (if we are leader)
            bool done = false;
            while (!done)
            {
                if (server->isRecovering())
                    break;

                if (shutdown_called)
                    return;

                if (isLeader())
                {
                    server->applyConfigurationUpdate(action);
                    done = true;
                }
                else
                {
                    done = server->waitConfigurationUpdate(action);
                    if (!done)
                        LOG_INFO(log, "Cannot wait for configuration update, maybe we become leader, or maybe update is invalid, will try to wait one more time");
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

// Used to update the state for a session based on the requests
// - update the number of current unprocessed requests for the session
// - if the number of unprocessed requests is 0, we can start adding next type of requests
//   from unprocessed requests queue to the active queue
void KeeperDispatcher::finalizeRequests(const KeeperStorage::RequestsForSessions & requests_for_sessions)
{
    std::unordered_map<int64_t, size_t> counts_for_session;

    for (const auto & request_for_session : requests_for_sessions)
    {
        ++counts_for_session[request_for_session.session_id];
    }

    std::lock_guard lock{unprocessed_request_mutex};
    for (const auto [session_id, count] : counts_for_session)
    {
        auto unprocessed_requests_it = unprocessed_requests_for_session.find(session_id);
        if (unprocessed_requests_it == unprocessed_requests_for_session.end())
            continue;

        auto & unprocessed_requests = unprocessed_requests_it->second;
        unprocessed_requests.unprocessed_num -= count;

        if (unprocessed_requests.unprocessed_num == 0)
        {
            if (!unprocessed_requests.request_queue.empty())
            {
                auto & unprocessed_requests_queue = unprocessed_requests.request_queue;
                unprocessed_requests.is_read = !unprocessed_requests.is_read;
                // start adding next type of requests
                while (!unprocessed_requests_queue.empty() && unprocessed_requests_queue.front().request->isReadRequest() == unprocessed_requests.is_read)
                {
                    auto & front_request = unprocessed_requests_queue.front();

                    /// Put close requests without timeouts
                    if (front_request.request->getOpNum() == Coordination::OpNum::Close)
                    {
                        if (!active_requests_queue->push(std::move(front_request)))
                            throw Exception("Cannot push request to queue", ErrorCodes::SYSTEM_ERROR);
                    }
                    else if (!active_requests_queue->tryPush(std::move(front_request), configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds()))
                    {
                        throw Exception("Cannot push request to queue within operation timeout", ErrorCodes::TIMEOUT_EXCEEDED);
                    }

                    ++unprocessed_requests.unprocessed_num;
                    unprocessed_requests_queue.pop_front();
                }
            }
            else
            {
                unprocessed_requests_for_session.erase(unprocessed_requests_it);
            }
        }
    }
}

// Finalize request
// Process read requests that were waiting for this commit
void KeeperDispatcher::onRequestCommit(const KeeperStorage::RequestForSession & request_for_session, uint64_t log_term, uint64_t log_idx)
{
    if (!finalize_requests_queue.push({request_for_session}))
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push read requests to queue");

    KeeperStorage::RequestsForSessions requests;
    {
        std::lock_guard lock(leader_waiter_mutex);
        auto request_queue_it = leader_waiters.find(KeeperServer::NodeInfo{.term = log_term, .last_committed_index = log_idx});
        if (request_queue_it != leader_waiters.end())
        {
            requests = std::move(request_queue_it->second);
            leader_waiters.erase(request_queue_it);
        }
    }

    if (requests.empty())
        return;

    if (!read_requests_queue.push(std::move(requests)))
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push read requests to queue");
}

/// Process all read request that are waiting for lower or currently last processed log index
void KeeperDispatcher::onApplySnapshot(uint64_t term, uint64_t last_idx)
{
    KeeperServer::NodeInfo current_node_info{term, last_idx};
    KeeperStorage::RequestsForSessions requests;
    {
        std::lock_guard lock(leader_waiter_mutex);
        for (auto leader_waiter_it = leader_waiters.begin(); leader_waiter_it != leader_waiters.end();)
        {
            auto waiting_node_info = leader_waiter_it->first;
            if (waiting_node_info.term <= current_node_info.term
                && waiting_node_info.last_committed_index <= current_node_info.last_committed_index)
            {
                for (auto & request : leader_waiter_it->second)
                {
                    requests.push_back(std::move(request));
                }

                leader_waiter_it = leader_waiters.erase(leader_waiter_it);
            }
            else
            {
                ++leader_waiter_it;
            }
        }
    }

    if (requests.empty())
        return;

    if (!read_requests_queue.push(std::move(requests)))
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push read requests to queue");
}

bool KeeperDispatcher::isServerActive() const
{
    return checkInit() && hasLeader() && !server->isRecovering();
}

void KeeperDispatcher::updateConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    auto diff = server->getConfigurationDiff(config);
    if (diff.empty())
        LOG_TRACE(log, "Configuration update triggered, but nothing changed for RAFT");
    else if (diff.size() > 1)
        LOG_WARNING(log, "Configuration changed for more than one server ({}) from cluster, it's strictly not recommended", diff.size());
    else
        LOG_DEBUG(log, "Configuration change size ({})", diff.size());

    for (auto & change : diff)
    {
        bool push_result = update_configuration_queue.push(change);
        if (!push_result)
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push configuration update to queue");
    }

#if USE_AWS_S3
    updateS3Configuration(config);
#endif
}

void KeeperDispatcher::updateKeeperStatLatency(uint64_t process_time_ms)
{
    keeper_stats.updateLatency(process_time_ms);
}

static uint64_t getDirSize(const fs::path & dir)
{
    checkStackSize();
    if (!fs::exists(dir))
        return 0;

    fs::directory_iterator it(dir);
    fs::directory_iterator end;

    uint64_t size{0};
    while (it != end)
    {
        if (it->is_regular_file())
            size += fs::file_size(*it);
        else
            size += getDirSize(it->path());
        ++it;
    }
    return size;
}

uint64_t KeeperDispatcher::getLogDirSize() const
{
    return getDirSize(configuration_and_settings->log_storage_path);
}

uint64_t KeeperDispatcher::getSnapDirSize() const
{
    return getDirSize(configuration_and_settings->snapshot_storage_path);
}

Keeper4LWInfo KeeperDispatcher::getKeeper4LWInfo() const
{
    Keeper4LWInfo result = server->getPartiallyFilled4LWInfo();
    {
        std::lock_guard lock(push_request_mutex);
        result.outstanding_requests_count = active_requests_queue->size();
    }
    {
        std::lock_guard lock(session_to_response_callback_mutex);
        result.alive_connections_count = session_to_response_callback.size();
    }
    return result;
}

}
