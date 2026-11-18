#include <Server/Kafka/KafkaHandler.h>
#include <Server/Kafka/KafkaError.h>
#include <Server/Kafka/KafkaGroupCoordinator.h>

#include <Core/ServerSettings.h>
#include <Core/UUID.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Session.h>
#include <Server/IServer.h>
#include <Server/TCPServer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <base/scope_guard.h>

#include <poll.h>

#include <chrono>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_POLL;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int TABLE_ALREADY_EXISTS;
}

namespace ServerSetting
{
    extern const ServerSettingsString default_session_user;
}

using namespace Kafka;

namespace
{

struct ApiVersionRange
{
    ApiKey key;
    Int16 min_version;
    Int16 max_version;
    /// The first version with the compact encodings and tagged fields.
    Int16 flexible_version;
};

constexpr Int16 never_flexible = std::numeric_limits<Int16>::max();

/// The versions are limited to the ones that identify topics by names, not by ids.
constexpr ApiVersionRange supported_apis[] =
{
    {ApiKey::Produce, 3, 9, 9},
    {ApiKey::Fetch, 4, 12, 12},
    {ApiKey::ListOffsets, 1, 6, 6},
    {ApiKey::Metadata, 0, 12, 9},
    {ApiKey::OffsetCommit, 2, 8, 8},
    {ApiKey::OffsetFetch, 1, 7, 6},
    {ApiKey::FindCoordinator, 0, 3, 3},
    {ApiKey::JoinGroup, 0, 7, 6},
    {ApiKey::Heartbeat, 0, 4, 4},
    {ApiKey::LeaveGroup, 0, 4, 4},
    {ApiKey::SyncGroup, 0, 5, 4},
    {ApiKey::DescribeGroups, 0, 5, 5},
    {ApiKey::ListGroups, 0, 3, 3},
    {ApiKey::SaslHandshake, 0, 1, never_flexible},
    {ApiKey::ApiVersions, 0, 4, 3},
    {ApiKey::CreateTopics, 2, 5, 5},
    {ApiKey::DeleteTopics, 1, 4, 4},
    {ApiKey::InitProducerId, 0, 4, 2},
    {ApiKey::SaslAuthenticate, 0, 2, 2},
};

const ApiVersionRange * findApi(ApiKey key)
{
    for (const auto & api : supported_apis)
        if (api.key == key)
            return &api;
    return nullptr;
}

/// This server is the only broker of the cluster.
constexpr Int32 node_id = 0;
constexpr Int32 leader_epoch = 0;
constexpr Int32 unknown_authorized_operations = std::numeric_limits<Int32>::min();

void writeApiVersionsBody(Writer & out, ErrorCode error, Int16 version)
{
    out.errorCode(error);
    out.arrayLength(std::size(supported_apis));
    for (const auto & api : supported_apis)
    {
        out.int16(static_cast<Int16>(api.key));
        out.int16(api.min_version);
        out.int16(api.max_version);
        out.taggedFields();
    }
    if (version >= 1)
        out.int32(0); /// throttle time
    out.taggedFields();
}

/// The SASL PLAIN message: [authzid] NUL authcid NUL passwd.
std::pair<String, String> parseSaslPlain(const String & token)
{
    auto first = token.find('\0');
    auto second = first == String::npos ? String::npos : token.find('\0', first + 1);
    if (second == String::npos)
        throw Error(ErrorCode::SASL_AUTHENTICATION_FAILED, ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Malformed SASL PLAIN message");
    return {token.substr(first + 1, second - first - 1), token.substr(second + 1)};
}

String exceptionMessage()
{
    return getCurrentExceptionMessage(/* with_stacktrace = */ false);
}

}


KafkaHandler::KafkaHandler(
    const Poco::Net::StreamSocket & socket_,
    IServer & server_,
    TCPServer & tcp_server_,
    const BrokerSettings & settings_,
    UInt64 connection_id_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , settings(settings_)
    , connection_id(connection_id_)
    , read_event(read_event_)
    , write_event(write_event_)
{
}

KafkaHandler::~KafkaHandler() = default;

bool KafkaHandler::isCancelled() const
{
    return !tcp_server.isOpen() || server.isCancelled();
}

void KafkaHandler::run()
{
    setThreadName(ThreadName::KAFKA_HANDLER);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket(), read_event);
    out = std::make_shared<WriteBufferFromPocoSocket>(socket(), write_event);
    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::KAFKA);

    SCOPE_EXIT({
        session.reset();
        out->cancel();
    });

    try
    {
        while (!isCancelled())
        {
            while (!in->poll(1000000))
                if (isCancelled())
                    return;

            if (in->eof())
                return;

            Int32 size = 0;
            readBinaryBigEndian(size, *in);
            if (size < 0 || static_cast<UInt64>(size) > settings.max_request_size)
            {
                LOG_WARNING(log, "Kafka request of size {} exceeds the limit of {} bytes, closing the connection", size, settings.max_request_size);
                return;
            }

            String request(size, '\0');
            in->readStrict(request.data(), size);

            if (expect_raw_sasl_token)
            {
                processSaslToken(request);
                continue;
            }

            if (!processRequest(request))
                return;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Kafka connection is closed because of an exception");
    }
}

bool KafkaHandler::processRequest(const String & request)
{
    Reader reader(request.data(), request.data() + request.size());

    RequestHeader header;
    header.api_key = static_cast<ApiKey>(reader.int16());
    header.api_version = reader.int16();
    header.correlation_id = reader.int32();
    /// The client id is never in the compact encoding.
    header.client_id = reader.nullableString().value_or("");

    String response;
    Writer writer(response);
    writer.int32(0); /// size, filled below
    writer.int32(header.correlation_id);

    auto send = [&]
    {
        Int32 size = static_cast<Int32>(response.size() - sizeof(Int32));
        String size_bytes;
        Writer(size_bytes).int32(size);
        memcpy(response.data(), size_bytes.data(), sizeof(Int32));
        out->write(response.data(), response.size());
        out->next();
    };

    const auto * api = findApi(header.api_key);
    if (!api || header.api_version < api->min_version || header.api_version > api->max_version)
    {
        if (header.api_key == ApiKey::ApiVersions)
        {
            /// The client retries with the version from this response, which has the layout of version 0.
            writeApiVersionsBody(writer, ErrorCode::UNSUPPORTED_VERSION, 0);
            send();
            return true;
        }

        LOG_WARNING(log, "Unsupported Kafka request: API key {}, version {}, closing the connection",
            static_cast<Int16>(header.api_key), header.api_version);
        return false;
    }

    const bool flexible = header.api_version >= api->flexible_version;
    reader.setFlexible(flexible);
    /// The tagged fields of the request header.
    reader.taggedFields();
    writer.setFlexible(flexible);

    /// The response header of ApiVersions has no tagged fields, so that a client can parse it before knowing the versions.
    if (flexible && header.api_key != ApiKey::ApiVersions)
        writer.taggedFields();

    if (!authenticated
        && header.api_key != ApiKey::ApiVersions
        && header.api_key != ApiKey::SaslHandshake
        && header.api_key != ApiKey::SaslAuthenticate)
        authenticateDefaultUserIfNeeded();

    bool keep_connection = true;
    switch (header.api_key)
    {
        case ApiKey::ApiVersions: apiVersions(header, reader, writer); break;
        case ApiKey::Metadata: metadata(header, reader, writer); break;
        case ApiKey::Produce:
            if (!produce(header, reader, writer))
                return true; /// No response for `acks = 0`.
            break;
        case ApiKey::Fetch: fetch(header, reader, writer); break;
        case ApiKey::ListOffsets: listOffsets(header, reader, writer); break;
        case ApiKey::FindCoordinator: findCoordinator(header, reader, writer); break;
        case ApiKey::JoinGroup: joinGroup(header, reader, writer); break;
        case ApiKey::SyncGroup: syncGroup(header, reader, writer); break;
        case ApiKey::Heartbeat: heartbeat(header, reader, writer); break;
        case ApiKey::LeaveGroup: leaveGroup(header, reader, writer); break;
        case ApiKey::OffsetCommit: offsetCommit(header, reader, writer); break;
        case ApiKey::OffsetFetch: offsetFetch(header, reader, writer); break;
        case ApiKey::ListGroups: listGroups(header, reader, writer); break;
        case ApiKey::DescribeGroups: describeGroups(header, reader, writer); break;
        case ApiKey::InitProducerId: initProducerId(header, reader, writer); break;
        case ApiKey::CreateTopics: createTopics(header, reader, writer); break;
        case ApiKey::DeleteTopics: deleteTopics(header, reader, writer); break;
        case ApiKey::SaslHandshake: saslHandshake(header, reader, writer); break;
        case ApiKey::SaslAuthenticate: keep_connection = saslAuthenticate(header, reader, writer); break;
    }

    send();
    return keep_connection;
}


void KafkaHandler::authenticate(const String & user, const String & password)
{
    String user_name = user;
    if (user_name.empty())
        user_name = server.context()->getServerSettings()[ServerSetting::default_session_user];

    session->authenticate(user_name, password, socket().peerAddress());
    session->makeSessionContext();
    authenticated = true;
}

void KafkaHandler::authenticateDefaultUserIfNeeded()
{
    if (authenticated)
        return;

    if (sasl_handshake_done)
        throw Error(ErrorCode::ILLEGAL_SASL_STATE, ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Kafka request before the end of SASL authentication");

    /// A client without SASL authentication connects as the default user without a password.
    authenticate("", "");
}

ContextMutablePtr KafkaHandler::makeQueryContext()
{
    auto context = session->makeQueryContext();
    context->setCurrentQueryId(fmt::format("kafka:{}:{}", connection_id, toString(UUIDHelpers::generateV4())));
    context->setSetting("enable_streaming_queries", Field(true));
    context->setSetting("use_skip_indexes_on_data_read", Field(false));
    if (!settings.log_queries)
        context->setSetting("log_queries", Field(false));
    return context;
}

String KafkaHandler::advertisedHost()
{
    if (!settings.advertised_host.empty())
        return settings.advertised_host;

    String host = socket().address().host().toString();
    /// An IPv4 address in the form of an IPv6 address.
    if (host.starts_with("::ffff:") && host.contains('.'))
        host = host.substr(strlen("::ffff:"));
    return host;
}

Int32 KafkaHandler::advertisedPort()
{
    if (settings.advertised_port)
        return settings.advertised_port;
    return socket().address().port();
}


void KafkaHandler::apiVersions(const RequestHeader & header, Reader & request, Writer & response)
{
    if (header.api_version >= 3)
    {
        request.string(); /// client software name
        request.string(); /// client software version
        request.taggedFields();
    }
    writeApiVersionsBody(response, ErrorCode::NONE, header.api_version);
}

void KafkaHandler::saslHandshake(const RequestHeader & header, Reader & request, Writer & response)
{
    String mechanism = request.string();

    if (authenticated || sasl_handshake_done)
    {
        response.errorCode(ErrorCode::ILLEGAL_SASL_STATE);
    }
    else if (mechanism != "PLAIN")
    {
        response.errorCode(ErrorCode::UNSUPPORTED_SASL_MECHANISM);
    }
    else
    {
        response.errorCode(ErrorCode::NONE);
        sasl_handshake_done = true;
        expect_raw_sasl_token = header.api_version == 0;
    }

    response.arrayLength(1);
    response.string("PLAIN");
}

bool KafkaHandler::saslAuthenticate(const RequestHeader & header, Reader & request, Writer & response)
{
    String token = request.bytes();
    request.taggedFields();

    ErrorCode error = ErrorCode::NONE;
    std::optional<String> error_message;

    if (!sasl_handshake_done || authenticated)
    {
        error = ErrorCode::ILLEGAL_SASL_STATE;
        error_message = "SaslAuthenticate request without a SaslHandshake";
    }
    else
    {
        try
        {
            auto [user, password] = parseSaslPlain(token);
            authenticate(user, password);
        }
        catch (...)
        {
            tryLogCurrentException(log, "SASL authentication failed");
            error = ErrorCode::SASL_AUTHENTICATION_FAILED;
            error_message = "Authentication failed: invalid user or password";
        }
    }

    response.errorCode(error);
    response.nullableString(error_message);
    response.bytes("");
    if (header.api_version >= 1)
        response.int64(0); /// session lifetime
    response.taggedFields();

    /// The connection is closed after a failed authentication.
    return error == ErrorCode::NONE;
}

void KafkaHandler::processSaslToken(const String & token)
{
    expect_raw_sasl_token = false;
    auto [user, password] = parseSaslPlain(token);
    authenticate(user, password);

    /// An empty SASL token as the response.
    String response;
    Writer(response).int32(0);
    out->write(response.data(), response.size());
    out->next();
}


void KafkaHandler::metadata(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    std::optional<std::vector<String>> requested_topics;
    Int32 num_topics = request.arrayLength();
    if (num_topics >= 0 && !(version == 0 && num_topics == 0))
    {
        requested_topics.emplace();
        for (Int32 i = 0; i < num_topics; ++i)
        {
            if (version >= 10)
                request.uuid();
            auto name = version >= 10 ? request.nullableString() : request.string();
            request.taggedFields();
            /// Topics by ids are not supported.
            if (name)
                requested_topics->push_back(std::move(*name));
        }
    }

    bool allow_auto_topic_creation = true;
    if (version >= 4)
        allow_auto_topic_creation = request.boolean();
    if (version >= 8 && version <= 10)
        request.boolean();
    if (version >= 8)
        request.boolean();
    request.taggedFields();

    auto context = session->sessionContext();
    std::vector<TopicResolution> topics;
    if (requested_topics)
    {
        for (const auto & name : *requested_topics)
        {
            auto resolution = resolveTopic(name, settings, context);
            if (resolution.error == ErrorCode::UNKNOWN_TOPIC_OR_PARTITION && settings.auto_create_topics && allow_auto_topic_creation)
            {
                try
                {
                    createTopic(name, settings.default_num_partitions, {}, false, settings, [this] { return makeQueryContext(); });
                }
                catch (const Exception & e)
                {
                    if (e.code() != ErrorCodes::TABLE_ALREADY_EXISTS)
                        tryLogCurrentException(log, fmt::format("Cannot create topic {}", name));
                }
                resolution = resolveTopic(name, settings, context);
            }

            if (!resolution.topic)
            {
                auto missing_topic = std::make_shared<Topic>();
                missing_topic->name = name;
                resolution.topic = std::move(missing_topic);
            }
            topics.push_back(std::move(resolution));
        }
    }
    else
    {
        for (const auto & name : listTopics(settings, context))
        {
            auto resolution = resolveTopic(name, settings, context);
            if (resolution.topic)
                topics.push_back(std::move(resolution));
        }
    }

    if (version >= 3)
        response.int32(0); /// throttle time

    response.arrayLength(1);
    response.int32(node_id);
    response.string(advertisedHost());
    response.int32(advertisedPort());
    if (version >= 1)
        response.nullString(); /// rack
    response.taggedFields();

    if (version >= 2)
        response.nullableString(settings.cluster_id);
    if (version >= 1)
        response.int32(node_id); /// controller

    response.arrayLength(topics.size());
    for (const auto & resolution : topics)
    {
        const Topic & topic = *resolution.topic;
        response.errorCode(resolution.error);
        response.string(topic.name);
        if (version >= 10)
            response.uuid();
        if (version >= 1)
            response.boolean(false); /// is internal

        Int32 num_partitions = resolution.error == ErrorCode::NONE ? topic.num_partitions : 0;
        response.arrayLength(num_partitions);
        for (Int32 partition = 0; partition < num_partitions; ++partition)
        {
            response.errorCode(ErrorCode::NONE);
            response.int32(partition);
            response.int32(node_id); /// leader
            if (version >= 7)
                response.int32(leader_epoch);
            response.arrayLength(1);
            response.int32(node_id); /// replicas
            response.arrayLength(1);
            response.int32(node_id); /// in-sync replicas
            if (version >= 5)
                response.arrayLength(0); /// offline replicas
            response.taggedFields();
        }

        if (version >= 8)
            response.int32(unknown_authorized_operations);
        response.taggedFields();
    }

    if (version >= 8 && version <= 10)
        response.int32(unknown_authorized_operations);
    response.taggedFields();
}


bool KafkaHandler::produce(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    auto transactional_id = request.nullableString();
    Int16 acks = request.int16();
    request.int32(); /// timeout

    struct PartitionResult
    {
        Int32 partition = 0;
        ErrorCode error = ErrorCode::NONE;
        std::optional<String> error_message;
    };

    std::vector<std::pair<String, std::vector<PartitionResult>>> results;
    auto context = session->sessionContext();
    auto make_context = [this] { return makeQueryContext(); };

    Int32 num_topics = request.arrayLength();
    for (Int32 i = 0; i < num_topics; ++i)
    {
        String name = request.string();
        auto resolution = resolveTopic(name, settings, context);
        auto & [_, partition_results] = results.emplace_back(name, std::vector<PartitionResult>{});

        Int32 num_partitions = request.arrayLength();
        for (Int32 j = 0; j < num_partitions; ++j)
        {
            PartitionResult & result = partition_results.emplace_back();
            result.partition = request.int32();
            auto records = request.nullableBytes();
            request.taggedFields();

            if (transactional_id)
            {
                result.error = ErrorCode::TRANSACTIONAL_ID_AUTHORIZATION_FAILED;
                result.error_message = "Transactions are not supported";
                continue;
            }

            if (!resolution.topic)
            {
                result.error = resolution.error;
                result.error_message = resolution.error_message;
                continue;
            }

            if (!resolution.topic->hasPartition(result.partition))
            {
                result.error = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                continue;
            }

            try
            {
                if (records)
                    Kafka::produce(*resolution.topic, result.partition, parseRecordBatches(*records), make_context);
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Cannot write to Kafka topic {} partition {}", name, result.partition));
                result.error = getCurrentExceptionKafkaErrorCode();
                result.error_message = exceptionMessage();
            }
        }
        request.taggedFields();
    }
    request.taggedFields();

    if (acks == 0)
        return false;

    response.arrayLength(results.size());
    for (const auto & [name, partition_results] : results)
    {
        response.string(name);
        response.arrayLength(partition_results.size());
        for (const auto & result : partition_results)
        {
            response.int32(result.partition);
            response.errorCode(result.error);
            /// The offsets of the inserted records are not known: the block number is assigned in the insert.
            response.int64(-1); /// base offset
            response.int64(-1); /// log append time
            if (version >= 5)
                response.int64(-1); /// log start offset
            if (version >= 8)
            {
                response.arrayLength(0); /// record errors
                response.nullableString(result.error_message);
            }
            response.taggedFields();
        }
        response.taggedFields();
    }
    response.int32(0); /// throttle time
    response.taggedFields();
    return true;
}


void KafkaHandler::fetch(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    request.int32(); /// replica id
    Int32 max_wait_ms = request.int32();
    Int32 min_bytes = request.int32();
    Int32 max_bytes = request.int32();
    request.int8(); /// isolation level: there are no transactions, so all records are committed
    if (version >= 7)
    {
        request.int32(); /// session id
        request.int32(); /// session epoch
    }

    struct TopicRequest
    {
        String name;
        TopicResolution resolution;
        std::vector<FetchPartitionRequest> partitions;
        std::vector<FetchPartitionResult> results;
        MergeTreeBoundsSubscriptionPtr subscription;
    };

    std::vector<TopicRequest> topics;
    auto context = session->sessionContext();

    Int32 num_topics = request.arrayLength();
    for (Int32 i = 0; i < num_topics; ++i)
    {
        TopicRequest & topic = topics.emplace_back();
        topic.name = request.string();
        topic.resolution = resolveTopic(topic.name, settings, context);

        Int32 num_partitions = request.arrayLength();
        for (Int32 j = 0; j < num_partitions; ++j)
        {
            FetchPartitionRequest & partition = topic.partitions.emplace_back();
            partition.partition = request.int32();
            if (version >= 9)
                request.int32(); /// current leader epoch
            partition.offset = request.int64();
            if (version >= 12)
                request.int32(); /// last fetched epoch
            if (version >= 5)
                request.int64(); /// log start offset
            partition.max_bytes = request.int32();
            request.taggedFields();
        }
        request.taggedFields();
    }

    if (version >= 7)
    {
        Int32 num_forgotten = request.arrayLength();
        for (Int32 i = 0; i < num_forgotten; ++i)
        {
            request.string();
            Int32 num_partitions = request.arrayLength();
            for (Int32 j = 0; j < num_partitions; ++j)
                request.int32();
            request.taggedFields();
        }
    }
    if (version >= 11)
        request.string(); /// rack id
    request.taggedFields();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(std::max(max_wait_ms, 0));
    const bool may_wait = max_wait_ms > 0 && min_bytes > 0;

    /// The subscriptions are registered before reading, so that no commit is missed between the read and the wait.
    if (may_wait)
    {
        for (auto & topic : topics)
        {
            if (!topic.resolution.topic)
                continue;
            topic.subscription = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
            topic.resolution.topic->data->subscription_manager.registerSubscription(topic.subscription);
            topic.resolution.topic->data->triggerStreamingSubscriptionEnrichment();
        }
    }

    auto make_context = [this] { return makeQueryContext(); };

    while (true)
    {
        size_t total_bytes = 0;
        size_t remaining_bytes = max_bytes > 0 ? static_cast<size_t>(max_bytes) : 1;
        for (auto & topic : topics)
        {
            if (!topic.resolution.topic)
                continue;

            try
            {
                topic.results = Kafka::fetch(*topic.resolution.topic, topic.partitions, remaining_bytes, make_context);
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Cannot read from Kafka topic {}", topic.name));
                ErrorCode error = getCurrentExceptionKafkaErrorCode();
                topic.results.assign(topic.partitions.size(), {});
                for (size_t i = 0; i < topic.partitions.size(); ++i)
                {
                    topic.results[i].partition = topic.partitions[i].partition;
                    topic.results[i].error = error;
                }
            }

            for (const auto & result : topic.results)
            {
                total_bytes += result.records.size();
                remaining_bytes -= std::min(remaining_bytes, result.records.size());
            }
            /// At least one record is returned, even if it is larger than the limit.
            remaining_bytes = std::max<size_t>(remaining_bytes, 1);
        }

        if (!may_wait || total_bytes >= static_cast<size_t>(min_bytes))
            break;

        /// Wait for a commit into any of the topics.
        bool changed = false;
        while (!changed)
        {
            auto now = std::chrono::steady_clock::now();
            if (now >= deadline || isCancelled())
                break;

            std::vector<pollfd> fds;
            std::vector<TopicRequest *> fd_topics;
            for (auto & topic : topics)
            {
                if (topic.subscription)
                {
                    fds.push_back({topic.subscription->fd(), POLLIN, 0});
                    fd_topics.push_back(&topic);
                }
            }
            if (fds.empty())
                break;

            auto timeout = std::min(std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now), std::chrono::milliseconds(1000));
            int res = ::poll(fds.data(), fds.size(), static_cast<int>(std::max<Int64>(timeout.count(), 1)));
            if (res < 0)
            {
                if (errno == EINTR)
                    continue;
                throw ErrnoException(ErrorCodes::CANNOT_POLL, "Cannot poll the streaming subscriptions");
            }

            for (size_t i = 0; i < fds.size(); ++i)
            {
                if (!(fds[i].revents & POLLIN))
                    continue;

                TopicRequest & topic = *fd_topics[i];
                topic.subscription->drain();
                auto snapshot = topic.subscription->snapshot();
                for (const auto & result : topic.results)
                {
                    auto it = snapshot.safe_block_numbers.find(topic.resolution.topic->partitionId(result.partition));
                    if (it != snapshot.safe_block_numbers.end() && it->second > result.safe_block_number)
                        changed = true;
                }
            }
        }

        if (!changed)
            break;
    }

    response.int32(0); /// throttle time
    if (version >= 7)
    {
        response.errorCode(ErrorCode::NONE);
        response.int32(0); /// session id: fetch sessions are not supported, the client sends full requests
    }

    response.arrayLength(topics.size());
    for (const auto & topic : topics)
    {
        response.string(topic.name);
        response.arrayLength(topic.partitions.size());
        for (size_t i = 0; i < topic.partitions.size(); ++i)
        {
            FetchPartitionResult result;
            if (topic.resolution.topic)
                result = topic.results[i];
            else
            {
                result.partition = topic.partitions[i].partition;
                result.error = topic.resolution.error;
            }

            response.int32(result.partition);
            response.errorCode(result.error);
            response.int64(result.high_watermark);
            response.int64(result.high_watermark); /// last stable offset
            if (version >= 5)
                response.int64(result.log_start_offset);
            response.arrayLength(0); /// aborted transactions
            if (version >= 11)
                response.int32(-1); /// preferred read replica
            response.bytes(result.records);
            response.taggedFields();
        }
        response.taggedFields();
    }
    response.taggedFields();
}


void KafkaHandler::listOffsets(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    request.int32(); /// replica id
    if (version >= 2)
        request.int8(); /// isolation level

    struct PartitionResult
    {
        Int32 partition = 0;
        ErrorCode error = ErrorCode::NONE;
        OffsetForTimestamp offset;
    };

    std::vector<std::pair<String, std::vector<PartitionResult>>> results;
    auto context = session->sessionContext();
    auto make_context = [this] { return makeQueryContext(); };

    Int32 num_topics = request.arrayLength();
    for (Int32 i = 0; i < num_topics; ++i)
    {
        String name = request.string();
        auto resolution = resolveTopic(name, settings, context);
        auto & [_, partition_results] = results.emplace_back(name, std::vector<PartitionResult>{});

        Int32 num_partitions = request.arrayLength();
        for (Int32 j = 0; j < num_partitions; ++j)
        {
            PartitionResult & result = partition_results.emplace_back();
            result.partition = request.int32();
            if (version >= 4)
                request.int32(); /// current leader epoch
            Int64 timestamp = request.int64();
            request.taggedFields();

            if (!resolution.topic)
            {
                result.error = resolution.error;
                continue;
            }
            if (!resolution.topic->hasPartition(result.partition))
            {
                result.error = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                continue;
            }

            try
            {
                result.offset = getOffsetForTimestamp(*resolution.topic, result.partition, timestamp, make_context);
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Cannot list offsets of Kafka topic {}", name));
                result.error = getCurrentExceptionKafkaErrorCode();
            }
        }
        request.taggedFields();
    }
    request.taggedFields();

    if (version >= 2)
        response.int32(0); /// throttle time

    response.arrayLength(results.size());
    for (const auto & [name, partition_results] : results)
    {
        response.string(name);
        response.arrayLength(partition_results.size());
        for (const auto & result : partition_results)
        {
            response.int32(result.partition);
            response.errorCode(result.error);
            response.int64(result.offset.timestamp);
            response.int64(result.offset.offset);
            if (version >= 4)
                response.int32(leader_epoch);
            response.taggedFields();
        }
        response.taggedFields();
    }
    response.taggedFields();
}


void KafkaHandler::findCoordinator(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    request.string(); /// key
    Int8 key_type = 0;
    if (version >= 1)
        key_type = request.int8();
    request.taggedFields();

    if (version >= 1)
        response.int32(0); /// throttle time

    if (key_type != 0)
    {
        response.errorCode(ErrorCode::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        if (version >= 1)
            response.string("Transactions are not supported");
        response.int32(-1);
        response.string("");
        response.int32(-1);
    }
    else
    {
        response.errorCode(ErrorCode::NONE);
        if (version >= 1)
            response.nullString();
        response.int32(node_id);
        response.string(advertisedHost());
        response.int32(advertisedPort());
    }
    response.taggedFields();
}


void KafkaHandler::joinGroup(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    GroupCoordinator::JoinRequest join;
    join.group_id = request.string();
    join.session_timeout_ms = request.int32();
    join.rebalance_timeout_ms = version >= 1 ? request.int32() : join.session_timeout_ms;
    join.member_id = request.string();
    if (version >= 5)
        join.group_instance_id = request.nullableString();
    join.protocol_type = request.string();

    Int32 num_protocols = request.arrayLength();
    for (Int32 i = 0; i < num_protocols; ++i)
    {
        String name = request.string();
        String metadata = request.bytes();
        request.taggedFields();
        join.protocols.emplace_back(std::move(name), std::move(metadata));
    }
    request.taggedFields();

    join.client_id = header.client_id;
    join.client_host = "/" + socket().peerAddress().host().toString();

    auto result = GroupCoordinator::instance().joinGroup(join, settings.group_initial_rebalance_delay_ms, [this] { return isCancelled(); });
    if (result.member_id.empty())
        result.member_id = join.member_id;

    if (version >= 2)
        response.int32(0); /// throttle time
    response.errorCode(result.error);
    response.int32(result.generation_id);
    if (version >= 7)
    {
        response.nullableString(result.error == ErrorCode::NONE ? std::optional<String>(result.protocol_type) : std::nullopt);
        response.nullableString(result.protocol_name);
    }
    else
    {
        response.string(result.protocol_name.value_or(""));
    }
    response.string(result.leader);
    response.string(result.member_id);

    response.arrayLength(result.members.size());
    for (const auto & member : result.members)
    {
        response.string(member.member_id);
        if (version >= 5)
            response.nullableString(member.group_instance_id);
        response.bytes(member.metadata);
        response.taggedFields();
    }
    response.taggedFields();
}

void KafkaHandler::syncGroup(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    String group_id = request.string();
    Int32 generation_id = request.int32();
    String member_id = request.string();
    if (version >= 3)
        request.nullableString(); /// group instance id
    if (version >= 5)
    {
        request.nullableString(); /// protocol type
        request.nullableString(); /// protocol name
    }

    std::vector<std::pair<String, String>> assignments;
    Int32 num_assignments = request.arrayLength();
    for (Int32 i = 0; i < num_assignments; ++i)
    {
        String assigned_member = request.string();
        String assignment = request.bytes();
        request.taggedFields();
        assignments.emplace_back(std::move(assigned_member), std::move(assignment));
    }
    request.taggedFields();

    auto result = GroupCoordinator::instance().syncGroup(group_id, generation_id, member_id, assignments, [this] { return isCancelled(); });

    if (version >= 1)
        response.int32(0); /// throttle time
    response.errorCode(result.error);
    if (version >= 5)
    {
        response.nullableString(result.protocol_type);
        response.nullableString(result.protocol_name);
    }
    response.bytes(result.assignment);
    response.taggedFields();
}

void KafkaHandler::heartbeat(const RequestHeader & header, Reader & request, Writer & response)
{
    String group_id = request.string();
    Int32 generation_id = request.int32();
    String member_id = request.string();
    if (header.api_version >= 3)
        request.nullableString(); /// group instance id
    request.taggedFields();

    ErrorCode error = GroupCoordinator::instance().heartbeat(group_id, generation_id, member_id);

    if (header.api_version >= 1)
        response.int32(0); /// throttle time
    response.errorCode(error);
    response.taggedFields();
}

void KafkaHandler::leaveGroup(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    String group_id = request.string();
    std::vector<std::pair<String, std::optional<String>>> members;
    if (version <= 2)
    {
        members.emplace_back(request.string(), std::nullopt);
    }
    else
    {
        Int32 num_members = request.arrayLength();
        for (Int32 i = 0; i < num_members; ++i)
        {
            String member_id = request.string();
            auto group_instance_id = request.nullableString();
            request.taggedFields();
            members.emplace_back(std::move(member_id), std::move(group_instance_id));
        }
    }
    request.taggedFields();

    std::vector<ErrorCode> errors;
    for (const auto & [member_id, _] : members)
        errors.push_back(GroupCoordinator::instance().leaveGroup(group_id, member_id));

    if (version >= 1)
        response.int32(0); /// throttle time

    if (version <= 2)
    {
        response.errorCode(errors.front());
    }
    else
    {
        response.errorCode(ErrorCode::NONE);
        response.arrayLength(members.size());
        for (size_t i = 0; i < members.size(); ++i)
        {
            response.string(members[i].first);
            response.nullableString(members[i].second);
            response.errorCode(errors[i]);
            response.taggedFields();
        }
    }
    response.taggedFields();
}


void KafkaHandler::offsetCommit(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    String group_id = request.string();
    Int32 generation_id = request.int32();
    String member_id = request.string();
    if (version >= 7)
        request.nullableString(); /// group instance id
    if (version <= 4)
        request.int64(); /// retention time

    std::vector<std::pair<String, std::vector<Int32>>> topics;
    std::vector<CommittedOffset> offsets;

    Int32 num_topics = request.arrayLength();
    for (Int32 i = 0; i < num_topics; ++i)
    {
        String name = request.string();
        auto & [_, partitions] = topics.emplace_back(name, std::vector<Int32>{});

        Int32 num_partitions = request.arrayLength();
        for (Int32 j = 0; j < num_partitions; ++j)
        {
            CommittedOffset & offset = offsets.emplace_back();
            offset.topic = name;
            offset.partition = request.int32();
            offset.offset = request.int64();
            if (version >= 6)
                request.int32(); /// committed leader epoch
            offset.metadata = request.nullableString().value_or("");
            request.taggedFields();
            partitions.push_back(offset.partition);
        }
        request.taggedFields();
    }
    request.taggedFields();

    ErrorCode error = GroupCoordinator::instance().validateOffsetCommit(group_id, generation_id, member_id);
    if (error == ErrorCode::NONE)
    {
        try
        {
            commitOffsets(group_id, offsets, settings, [this] { return makeQueryContext(); });
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Cannot commit offsets of the group {}", group_id));
            error = getCurrentExceptionKafkaErrorCode();
            if (error == ErrorCode::TOPIC_AUTHORIZATION_FAILED)
                error = ErrorCode::GROUP_AUTHORIZATION_FAILED;
        }
    }

    if (version >= 3)
        response.int32(0); /// throttle time

    response.arrayLength(topics.size());
    for (const auto & [name, partitions] : topics)
    {
        response.string(name);
        response.arrayLength(partitions.size());
        for (Int32 partition : partitions)
        {
            response.int32(partition);
            response.errorCode(error);
            response.taggedFields();
        }
        response.taggedFields();
    }
    response.taggedFields();
}

void KafkaHandler::offsetFetch(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    String group_id = request.string();

    std::optional<std::vector<std::pair<String, std::vector<Int32>>>> requested;
    Int32 num_topics = request.arrayLength();
    if (num_topics >= 0)
    {
        requested.emplace();
        for (Int32 i = 0; i < num_topics; ++i)
        {
            auto & [name, partitions] = requested->emplace_back(request.string(), std::vector<Int32>{});
            Int32 num_partitions = request.arrayLength();
            for (Int32 j = 0; j < num_partitions; ++j)
                partitions.push_back(request.int32());
            request.taggedFields();
        }
    }
    if (version >= 7)
        request.boolean(); /// require stable
    request.taggedFields();

    ErrorCode error = ErrorCode::NONE;
    std::map<std::pair<String, Int32>, CommittedOffset> committed;
    try
    {
        for (auto & offset : fetchCommittedOffsets(group_id, settings, [this] { return makeQueryContext(); }))
            committed[{offset.topic, offset.partition}] = std::move(offset);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Cannot fetch the committed offsets of the group {}", group_id));
        error = getCurrentExceptionKafkaErrorCode();
        if (error == ErrorCode::TOPIC_AUTHORIZATION_FAILED)
            error = ErrorCode::GROUP_AUTHORIZATION_FAILED;
    }

    if (!requested)
    {
        requested.emplace();
        for (const auto & [key, _] : committed)
        {
            if (requested->empty() || requested->back().first != key.first)
                requested->emplace_back(key.first, std::vector<Int32>{});
            requested->back().second.push_back(key.second);
        }
    }

    if (version >= 3)
        response.int32(0); /// throttle time

    response.arrayLength(requested->size());
    for (const auto & [name, partitions] : *requested)
    {
        response.string(name);
        response.arrayLength(partitions.size());
        for (Int32 partition : partitions)
        {
            auto it = committed.find({name, partition});
            response.int32(partition);
            response.int64(it != committed.end() ? it->second.offset : -1);
            if (version >= 5)
                response.int32(-1); /// committed leader epoch
            response.nullableString(it != committed.end() ? std::optional<String>(it->second.metadata) : std::optional<String>(""));
            /// Before version 2, there is no top level error code.
            response.errorCode(version < 2 ? error : ErrorCode::NONE);
            response.taggedFields();
        }
        response.taggedFields();
    }
    if (version >= 2)
        response.errorCode(error);
    response.taggedFields();
}


void KafkaHandler::listGroups(const RequestHeader & header, Reader & request, Writer & response)
{
    request.taggedFields();

    auto groups = GroupCoordinator::instance().listGroups();
    if (header.api_version >= 1)
        response.int32(0); /// throttle time
    response.errorCode(ErrorCode::NONE);
    response.arrayLength(groups.size());
    for (const auto & group : groups)
    {
        response.string(group.group_id);
        response.string(group.protocol_type);
        response.taggedFields();
    }
    response.taggedFields();
}

void KafkaHandler::describeGroups(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    std::vector<String> group_ids;
    Int32 num_groups = request.arrayLength();
    for (Int32 i = 0; i < num_groups; ++i)
        group_ids.push_back(request.string());
    if (version >= 3)
        request.boolean(); /// include authorized operations
    request.taggedFields();

    if (version >= 1)
        response.int32(0); /// throttle time
    response.arrayLength(group_ids.size());
    for (const auto & group_id : group_ids)
    {
        auto description = GroupCoordinator::instance().describeGroup(group_id);
        response.errorCode(ErrorCode::NONE);
        response.string(description.group_id);
        response.string(description.state);
        response.string(description.protocol_type);
        response.string(description.protocol_name);
        response.arrayLength(description.members.size());
        for (const auto & member : description.members)
        {
            response.string(member.member_id);
            if (version >= 4)
                response.nullableString(member.group_instance_id);
            response.string(member.client_id);
            response.string(member.client_host);
            response.bytes(member.metadata);
            response.bytes(member.assignment);
            response.taggedFields();
        }
        if (version >= 3)
            response.int32(unknown_authorized_operations);
        response.taggedFields();
    }
    response.taggedFields();
}


void KafkaHandler::initProducerId(const RequestHeader & header, Reader & request, Writer & response)
{
    auto transactional_id = request.nullableString();
    request.int32(); /// transaction timeout
    Int64 producer_id = -1;
    Int16 producer_epoch = -1;
    if (header.api_version >= 3)
    {
        producer_id = request.int64();
        producer_epoch = request.int16();
    }
    request.taggedFields();

    response.int32(0); /// throttle time
    if (transactional_id)
    {
        response.errorCode(ErrorCode::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        response.int64(-1);
        response.int16(-1);
    }
    else if (producer_id >= 0 && producer_epoch >= 0 && producer_epoch < std::numeric_limits<Int16>::max())
    {
        /// A producer bumps its epoch after an error.
        response.errorCode(ErrorCode::NONE);
        response.int64(producer_id);
        response.int16(producer_epoch + 1);
    }
    else
    {
        response.errorCode(ErrorCode::NONE);
        response.int64(GroupCoordinator::instance().allocateProducerId());
        response.int16(0);
    }
    response.taggedFields();
}


void KafkaHandler::createTopics(const RequestHeader & header, Reader & request, Writer & response)
{
    const Int16 version = header.api_version;

    struct TopicRequest
    {
        String name;
        Int32 num_partitions = -1;
        std::vector<std::pair<String, std::optional<String>>> configs;
        ErrorCode error = ErrorCode::NONE;
        std::optional<String> error_message;
    };

    std::vector<TopicRequest> topics;
    Int32 num_topics = request.arrayLength();
    for (Int32 i = 0; i < num_topics; ++i)
    {
        TopicRequest & topic = topics.emplace_back();
        topic.name = request.string();
        topic.num_partitions = request.int32();
        request.int16(); /// replication factor: the replication is a property of the table engine

        Int32 num_assignments = request.arrayLength();
        for (Int32 j = 0; j < num_assignments; ++j)
        {
            request.int32();
            Int32 num_brokers = request.arrayLength();
            for (Int32 k = 0; k < num_brokers; ++k)
                request.int32();
            request.taggedFields();
        }

        Int32 num_configs = request.arrayLength();
        for (Int32 j = 0; j < num_configs; ++j)
        {
            String config_name = request.string();
            auto config_value = request.nullableString();
            request.taggedFields();
            topic.configs.emplace_back(std::move(config_name), std::move(config_value));
        }
        request.taggedFields();
    }
    request.int32(); /// timeout
    bool validate_only = request.boolean();
    request.taggedFields();

    auto context = session->sessionContext();
    for (auto & topic : topics)
    {
        if (resolveTopic(topic.name, settings, context).error != ErrorCode::UNKNOWN_TOPIC_OR_PARTITION)
        {
            topic.error = ErrorCode::TOPIC_ALREADY_EXISTS;
            topic.error_message = fmt::format("Topic {} already exists", topic.name);
            continue;
        }

        try
        {
            if (topic.num_partitions == -1)
                topic.num_partitions = settings.default_num_partitions;
            createTopic(topic.name, topic.num_partitions, topic.configs, validate_only, settings, [this] { return makeQueryContext(); });
        }
        catch (const Exception & e)
        {
            tryLogCurrentException(log, fmt::format("Cannot create topic {}", topic.name));
            topic.error = e.code() == ErrorCodes::TABLE_ALREADY_EXISTS ? ErrorCode::TOPIC_ALREADY_EXISTS : getCurrentExceptionKafkaErrorCode();
            topic.error_message = exceptionMessage();
        }
    }

    if (version >= 2)
        response.int32(0); /// throttle time
    response.arrayLength(topics.size());
    for (const auto & topic : topics)
    {
        response.string(topic.name);
        response.errorCode(topic.error);
        response.nullableString(topic.error_message);
        if (version >= 5)
        {
            response.int32(topic.error == ErrorCode::NONE ? topic.num_partitions : -1);
            response.int16(topic.error == ErrorCode::NONE ? 1 : -1); /// replication factor
            /// The configs of a created topic; clients expect an array on success and null on error.
            if (topic.error == ErrorCode::NONE)
                response.arrayLength(0);
            else
                response.nullArray();
        }
        response.taggedFields();
    }
    response.taggedFields();
}

void KafkaHandler::deleteTopics(const RequestHeader &, Reader & request, Writer & response)
{
    std::vector<String> names;
    Int32 num_topics = request.arrayLength();
    for (Int32 i = 0; i < num_topics; ++i)
        names.push_back(request.string());
    request.int32(); /// timeout
    request.taggedFields();

    auto context = session->sessionContext();
    response.int32(0); /// throttle time
    response.arrayLength(names.size());
    for (const auto & name : names)
    {
        ErrorCode error = ErrorCode::NONE;
        auto resolution = resolveTopic(name, settings, context);
        if (!resolution.topic)
        {
            error = resolution.error;
        }
        else
        {
            try
            {
                StorageID table_id = resolution.topic->table_id;
                resolution.topic.reset();
                deleteTopic(table_id, [this] { return makeQueryContext(); });
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Cannot delete topic {}", name));
                error = getCurrentExceptionKafkaErrorCode();
            }
        }

        response.string(name);
        response.errorCode(error);
        response.taggedFields();
    }
    response.taggedFields();
}

}
