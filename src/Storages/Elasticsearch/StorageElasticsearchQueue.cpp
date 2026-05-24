#include <Storages/Elasticsearch/StorageElasticsearchQueue.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/StoragePolicy.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromStreamLikeEngine.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/Elasticsearch/ElasticsearchQueueSettings.h>
#include <Storages/Elasticsearch/ElasticsearchQueueSource.h>
#include <Storages/Elasticsearch/ElasticsearchQueue_fwd.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StreamingStorageRegistry.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <filesystem>
#include <sstream>

namespace DB
{

namespace Setting
{
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsMilliseconds stream_flush_interval_ms;
    extern const SettingsMilliseconds stream_poll_timeout_ms;
    extern const SettingsBool use_concurrency_control;
}

namespace ElasticsearchQueueSetting
{
    extern const ElasticsearchQueueSettingsString elasticsearch_api_key;
    extern const ElasticsearchQueueSettingsString elasticsearch_auth_type;
    extern const ElasticsearchQueueSettingsString elasticsearch_bearer_token;
    extern const ElasticsearchQueueSettingsBool elasticsearch_commit_on_select;
    extern const ElasticsearchQueueSettingsMilliseconds elasticsearch_consumer_reschedule_ms;
    extern const ElasticsearchQueueSettingsString elasticsearch_cursor_field;
    extern const ElasticsearchQueueSettingsMilliseconds elasticsearch_flush_interval_ms;
    extern const ElasticsearchQueueSettingsString elasticsearch_index;
    extern const ElasticsearchQueueSettingsString elasticsearch_keeper_checkpoint_name;
    extern const ElasticsearchQueueSettingsString elasticsearch_keeper_path;
    extern const ElasticsearchQueueSettingsUInt64 elasticsearch_max_block_size;
    extern const ElasticsearchQueueSettingsString elasticsearch_password;
    extern const ElasticsearchQueueSettingsString elasticsearch_pit_keep_alive;
    extern const ElasticsearchQueueSettingsUInt64 elasticsearch_poll_max_batch_size;
    extern const ElasticsearchQueueSettingsMilliseconds elasticsearch_poll_timeout_ms;
    extern const ElasticsearchQueueSettingsString elasticsearch_query;
    extern const ElasticsearchQueueSettingsString elasticsearch_tiebreaker_field;
    extern const ElasticsearchQueueSettingsString elasticsearch_url;
    extern const ElasticsearchQueueSettingsBool elasticsearch_use_point_in_time;
    extern const ElasticsearchQueueSettingsString elasticsearch_user;
}

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NO_ZOOKEEPER;
    extern const int QUERY_NOT_ALLOWED;
}

namespace
{
    constexpr auto CHECKPOINT_FILE = "checkpoint.json";
    constexpr auto TMP_SUFFIX = ".tmp";

    enum class ElasticsearchAuthType
    {
        Auto,
        None,
        Basic,
        ApiKey,
        Bearer,
    };

    String toLowerASCII(String value)
    {
        std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return value;
    }

    ElasticsearchAuthType parseAuthType(const String & auth_type)
    {
        const String normalized = toLowerASCII(auth_type);
        if (normalized == "auto")
            return ElasticsearchAuthType::Auto;
        if (normalized == "none")
            return ElasticsearchAuthType::None;
        if (normalized == "basic")
            return ElasticsearchAuthType::Basic;
        if (normalized == "api_key")
            return ElasticsearchAuthType::ApiKey;
        if (normalized == "bearer")
            return ElasticsearchAuthType::Bearer;

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unsupported value '{}' for elasticsearch_auth_type. Supported values are: auto, none, basic, api_key, bearer",
            auth_type);
    }

    String serializeJSON(const Poco::Dynamic::Var & value)
    {
        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(value, oss);
        return oss.str();
    }

    String serializeJSONObject(const Poco::JSON::Object::Ptr & value)
    {
        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(*value, oss);
        return oss.str();
    }

    String makeEndpointURL(const String & base_url, const String & path)
    {
        String endpoint = base_url;
        if (!endpoint.ends_with('/'))
            endpoint += '/';
        endpoint += path;
        return endpoint;
    }

    String makeIndexEndpointURL(const String & base_url, const String & index, const String & path)
    {
        String endpoint = makeEndpointURL(base_url, index);
        if (!endpoint.ends_with('/'))
            endpoint += '/';
        endpoint += path;
        return endpoint;
    }

    String makeSearchURL(const String & base_url, const String & index)
    {
        return makeIndexEndpointURL(base_url, index, "_search");
    }

    Poco::JSON::Object::Ptr makeAscendingSortEntry(const String & field)
    {
        Poco::JSON::Object::Ptr order = new Poco::JSON::Object;
        order->set("order", "asc");

        Poco::JSON::Object::Ptr sort_entry = new Poco::JSON::Object;
        sort_entry->set(field, order);
        return sort_entry;
    }

    String getUpdatedPointInTimeId(const String & response, const String & current_pit_id)
    {
        Poco::JSON::Parser parser;
        auto parsed = parser.parse(response);
        if (parsed.type() != typeid(Poco::JSON::Object::Ptr))
            return current_pit_id;

        auto root = parsed.extract<Poco::JSON::Object::Ptr>();
        if (root->has("pit_id"))
            return root->getValue<String>("pit_id");

        return current_pit_id;
    }
}

class ReadFromStorageElasticsearchQueue final : public ReadFromStreamLikeEngine
{
public:
    ReadFromStorageElasticsearchQueue(
        const Names & column_names_,
        StoragePtr storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        SelectQueryInfo & query_info,
        ContextPtr context_)
        : ReadFromStreamLikeEngine{column_names_, storage_snapshot_, query_info.storage_limits, context_}
        , column_names{column_names_}
        , storage{storage_}
        , storage_snapshot{storage_snapshot_}
    {
    }

    String getName() const override { return "ReadFromStorageElasticsearchQueue"; }

private:
    Pipe makePipe() final
    {
        auto & elasticsearch_storage = storage->as<StorageElasticsearchQueue &>();

        if (elasticsearch_storage.shutdown_called)
            throw Exception(ErrorCodes::ABORTED, "Table is detached");

        if (elasticsearch_storage.mv_attached)
            throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageElasticsearchQueue with attached materialized views");

        Pipes pipes;
        pipes.emplace_back(std::make_shared<ElasticsearchQueueSource>(
            elasticsearch_storage,
            storage_snapshot,
            getContext(),
            column_names,
            elasticsearch_storage.getPollMaxBatchSize(),
            elasticsearch_storage.commitOnSelect()));

        return Pipe::unitePipes(std::move(pipes));
    }

    const Names column_names;
    StoragePtr storage;
    StorageSnapshotPtr storage_snapshot;
};

StorageElasticsearchQueue::StorageElasticsearchQueue(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & comment,
    std::unique_ptr<ElasticsearchQueueSettings> settings_,
    const String & relative_data_path_,
    LoadingStrictnessLevel mode,
    const String & collection_name_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , settings(std::move(settings_))
    , macros_info{.table_id = table_id_}
    , url(getContext()->getMacros()->expand((*settings)[ElasticsearchQueueSetting::elasticsearch_url].value, macros_info))
    , index(getContext()->getMacros()->expand((*settings)[ElasticsearchQueueSetting::elasticsearch_index].value, macros_info))
    , cursor_field(getContext()->getMacros()->expand((*settings)[ElasticsearchQueueSetting::elasticsearch_cursor_field].value, macros_info))
    , tiebreaker_field(getContext()->getMacros()->expand((*settings)[ElasticsearchQueueSetting::elasticsearch_tiebreaker_field].value, macros_info))
    , query_body(getContext()->getMacros()->expand((*settings)[ElasticsearchQueueSetting::elasticsearch_query].value, macros_info))
    , user(getContext()->getMacros()->expand((*settings)[ElasticsearchQueueSetting::elasticsearch_user].value, macros_info))
    , password((*settings)[ElasticsearchQueueSetting::elasticsearch_password].value)
    , api_key((*settings)[ElasticsearchQueueSetting::elasticsearch_api_key].value)
    , auth_type(getContext()->getMacros()->expand((*settings)[ElasticsearchQueueSetting::elasticsearch_auth_type].value, macros_info))
    , bearer_token((*settings)[ElasticsearchQueueSetting::elasticsearch_bearer_token].value)
    , use_point_in_time((*settings)[ElasticsearchQueueSetting::elasticsearch_use_point_in_time].value)
    , pit_keep_alive(getContext()->getMacros()->expand((*settings)[ElasticsearchQueueSetting::elasticsearch_pit_keep_alive].value, macros_info))
    , keeper_path(getContext()->getMacros()->expand((*settings)[ElasticsearchQueueSetting::elasticsearch_keeper_path].value, macros_info))
    , keeper_checkpoint_name(getContext()->getMacros()->expand((*settings)[ElasticsearchQueueSetting::elasticsearch_keeper_checkpoint_name].value, macros_info))
    , collection_name(collection_name_)
    , log(getLogger("StorageElasticsearchQueue (" + table_id_.getFullTableName() + ")"))
    , disk(getContext()->getStoragePolicy("default")->getDisks().at(0))
    , metadata_base_path(std::filesystem::path(relative_data_path_) / "elasticsearch_queue")
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);

    validateAuthenticationSettings();

    if (!disk->existsDirectory(metadata_base_path))
        disk->createDirectories(metadata_base_path);

    if (!keeper_path.empty())
        initializeKeeperCheckpoint();

    if (mode <= LoadingStrictnessLevel::ATTACH)
        loadCheckpoint();

    auto background_task = getContext()->getMessageBrokerSchedulePool().createTask(getStorageID(), log->name(), [this] { threadFunc(); });
    background_task->deactivate();
    task = std::make_shared<TaskContext>(std::move(background_task));
}

StorageElasticsearchQueue::~StorageElasticsearchQueue()
{
    if (!shutdown_called)
        shutdown(false);
}

std::string StorageElasticsearchQueue::getName() const
{
    return ElasticsearchQueue::TABLE_ENGINE_NAME;
}

VirtualColumnsDescription StorageElasticsearchQueue::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_id", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_index", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_score", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_source", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    return desc;
}

void StorageElasticsearchQueue::startup()
{
    if (task)
        task->holder->activateAndSchedule();
    StreamingStorageRegistry::instance().registerTable(getStorageID());
}

void StorageElasticsearchQueue::shutdown(bool)
{
    shutdown_called = true;
    if (task)
    {
        task->stream_cancelled = true;
        task->holder->deactivate();
    }

    StreamingStorageRegistry::instance().unregisterTable(getStorageID(), /* if_exists */ true);
}

void StorageElasticsearchQueue::drop()
{
    try
    {
        if (disk->existsDirectory(metadata_base_path))
        {
            LOG_INFO(log, "Removing ElasticsearchQueue metadata directory {}", metadata_base_path);
            disk->removeRecursive(metadata_base_path);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageElasticsearchQueue::renameInMemory(const StorageID & new_table_id)
{
    const auto prev_storage_id = getStorageID();
    IStorage::renameInMemory(new_table_id);
    StreamingStorageRegistry::instance().renameTable(prev_storage_id, getStorageID());
}

void StorageElasticsearchQueue::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum,
    size_t,
    size_t)
{
    query_plan.addStep(std::make_unique<ReadFromStorageElasticsearchQueue>(
        column_names, shared_from_this(), storage_snapshot, query_info, std::move(query_context)));
}

String StorageElasticsearchQueue::getCheckpointPath() const
{
    return std::filesystem::path(metadata_base_path) / CHECKPOINT_FILE;
}

void StorageElasticsearchQueue::loadCheckpoint()
{
    if (!keeper_path.empty())
    {
        checkpoint = loadKeeperCheckpoint();
        return;
    }

    const auto checkpoint_path = getCheckpointPath();
    if (!disk->existsFile(checkpoint_path))
        return;

    auto in = disk->readFile(checkpoint_path, getReadSettings());
    if (!in || in->eof())
        return;

    String loaded_checkpoint;
    readStringUntilEOF(loaded_checkpoint, *in);
    checkpoint = loaded_checkpoint;
}

void StorageElasticsearchQueue::storeCheckpoint(const String & new_checkpoint) const
{
    if (!keeper_path.empty())
    {
        storeKeeperCheckpoint(new_checkpoint);
        return;
    }

    const auto checkpoint_path = getCheckpointPath();
    const String tmp_path = checkpoint_path + TMP_SUFFIX;
    disk->removeFileIfExists(tmp_path);

    try
    {
        disk->createFile(tmp_path);
        auto out = disk->writeFile(tmp_path);
        writeString(new_checkpoint, *out);
        out->finalize();
    }
    catch (...)
    {
        disk->removeFileIfExists(tmp_path);
        throw;
    }

    disk->replaceFile(tmp_path, checkpoint_path);
}

String StorageElasticsearchQueue::getKeeperCheckpointPath() const
{
    return keeper_path + "/" + keeper_checkpoint_name;
}

String StorageElasticsearchQueue::getKeeperLockPath() const
{
    return keeper_path + "/lock";
}

zkutil::ZooKeeperPtr StorageElasticsearchQueue::getZooKeeper() const
{
    if (!getContext()->hasZooKeeper())
        throw Exception(ErrorCodes::NO_ZOOKEEPER, "ElasticsearchQueue requires Keeper when elasticsearch_keeper_path is configured");

    return getContext()->getZooKeeper();
}

void StorageElasticsearchQueue::initializeKeeperCheckpoint() const
{
    auto zookeeper = getZooKeeper();
    zookeeper->createAncestors(keeper_path);
    zookeeper->createIfNotExists(keeper_path, "");
    zookeeper->createIfNotExists(getKeeperCheckpointPath(), "");
}

String StorageElasticsearchQueue::loadKeeperCheckpoint() const
{
    initializeKeeperCheckpoint();

    String keeper_checkpoint;
    getZooKeeper()->tryGet(getKeeperCheckpointPath(), keeper_checkpoint);
    return keeper_checkpoint;
}

void StorageElasticsearchQueue::storeKeeperCheckpoint(const String & new_checkpoint, const zkutil::ZooKeeperPtr & zookeeper) const
{
    auto keeper = zookeeper ? zookeeper : getZooKeeper();
    keeper->createOrUpdate(getKeeperCheckpointPath(), new_checkpoint, zkutil::CreateMode::Persistent);
}

std::pair<zkutil::ZooKeeperPtr, zkutil::EphemeralNodeHolder::Ptr> StorageElasticsearchQueue::tryAcquireKeeperCheckpointLock() const
{
    initializeKeeperCheckpoint();
    auto zookeeper = getZooKeeper();
    auto lock_holder = zkutil::EphemeralNodeHolder::tryCreate(getKeeperLockPath(), *zookeeper, getStorageID().getNameForLogs());
    return {std::move(zookeeper), std::move(lock_holder)};
}

void StorageElasticsearchQueue::validateAuthenticationSettings() const
{
    const auto type = parseAuthType(auth_type);
    const bool has_basic_credentials = !user.empty() || !password.empty();
    const bool has_api_key = !api_key.empty();
    const bool has_bearer_token = !bearer_token.empty();

    if (has_basic_credentials && user.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_user must be specified when basic authentication is used");

    const size_t credentials_count = static_cast<size_t>(has_basic_credentials) + static_cast<size_t>(has_api_key) + static_cast<size_t>(has_bearer_token);
    if (type == ElasticsearchAuthType::Auto)
    {
        if (credentials_count > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_auth_type=auto cannot infer authentication when multiple credential types are configured");
        return;
    }

    if (type == ElasticsearchAuthType::None)
    {
        if (credentials_count != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_auth_type=none does not allow Elasticsearch credentials");
        return;
    }

    if (type == ElasticsearchAuthType::Basic)
    {
        if (user.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_auth_type=basic requires elasticsearch_user");
        if (has_api_key || has_bearer_token)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_auth_type=basic cannot be used with API key or bearer token credentials");
        return;
    }

    if (type == ElasticsearchAuthType::ApiKey)
    {
        if (api_key.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_auth_type=api_key requires elasticsearch_api_key");
        if (has_basic_credentials || has_bearer_token)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_auth_type=api_key cannot be used with basic or bearer token credentials");
        return;
    }

    if (type == ElasticsearchAuthType::Bearer)
    {
        if (bearer_token.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_auth_type=bearer requires elasticsearch_bearer_token");
        if (has_basic_credentials || has_api_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_auth_type=bearer cannot be used with basic or API key credentials");
    }
}

ConnectionTimeouts StorageElasticsearchQueue::getHTTPTimeouts() const
{
    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(getContext()->getSettingsRef(), getContext()->getServerSettings());

    const auto & engine_poll_timeout = (*settings)[ElasticsearchQueueSetting::elasticsearch_poll_timeout_ms];
    const Poco::Timespan poll_timeout = engine_poll_timeout.changed
        ? engine_poll_timeout
        : getContext()->getSettingsRef()[Setting::stream_poll_timeout_ms];
    if (poll_timeout.totalMilliseconds() > 0)
    {
        timeouts.withConnectionTimeout(poll_timeout);
        timeouts.withSendTimeout(poll_timeout);
        timeouts.withReceiveTimeout(poll_timeout);
    }

    return timeouts;
}

HTTPHeaderEntries StorageElasticsearchQueue::makeAuthHeaders() const
{
    HTTPHeaderEntries headers{
        {"Accept", "application/json"},
    };

    auto type = parseAuthType(auth_type);
    if (type == ElasticsearchAuthType::Auto)
    {
        if (!api_key.empty())
            type = ElasticsearchAuthType::ApiKey;
        else if (!bearer_token.empty())
            type = ElasticsearchAuthType::Bearer;
        else if (!user.empty() || !password.empty())
            type = ElasticsearchAuthType::Basic;
        else
            type = ElasticsearchAuthType::None;
    }

    if (type == ElasticsearchAuthType::ApiKey)
        headers.emplace_back("Authorization", "ApiKey " + api_key);
    else if (type == ElasticsearchAuthType::Bearer)
        headers.emplace_back("Authorization", "Bearer " + bearer_token);

    return headers;
}

void StorageElasticsearchQueue::setBasicCredentials(Poco::Net::HTTPBasicCredentials & credentials) const
{
    auto type = parseAuthType(auth_type);
    if (type == ElasticsearchAuthType::Auto && (!user.empty() || !password.empty()))
        type = ElasticsearchAuthType::Basic;

    if (type == ElasticsearchAuthType::Basic)
    {
        credentials.setUsername(user);
        credentials.setPassword(password);
    }
}

String StorageElasticsearchQueue::executeHTTPRequest(const String & method, const Poco::URI & uri, const String & body_string) const
{
    auto headers = makeAuthHeaders();
    if (!body_string.empty())
        headers.emplace_back("Content-Type", "application/json");

    ReadWriteBufferFromHTTP::OutStreamCallback out_callback;
    if (!body_string.empty())
    {
        out_callback = [body_string](std::ostream & output)
        {
            output << body_string;
        };
    }

    Poco::Net::HTTPBasicCredentials credentials;
    setBasicCredentials(credentials);
    auto buf = BuilderRWBufferFromHTTP(uri)
        .withMethod(method)
        .withSettings(getContext()->getReadSettings())
        .withTimeouts(getHTTPTimeouts())
        .withHostFilter(&getContext()->getRemoteHostFilter())
        .withOutCallback(std::move(out_callback))
        .withHeaders(std::move(headers))
        .withDelayInit(false)
        .create(credentials);

    String response;
    readStringUntilEOF(response, *buf);
    return response;
}

String StorageElasticsearchQueue::openPointInTime() const
{
    Poco::URI uri(makeIndexEndpointURL(url, index, "_pit"));
    uri.addQueryParameter("keep_alive", pit_keep_alive);

    const String response = executeHTTPRequest(Poco::Net::HTTPRequest::HTTP_POST, uri, "");
    Poco::JSON::Parser parser;
    auto parsed = parser.parse(response);
    if (parsed.type() != typeid(Poco::JSON::Object::Ptr))
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Elasticsearch point-in-time response must be a JSON object");

    auto root = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!root->has("id"))
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Elasticsearch point-in-time response does not contain 'id'");

    return root->getValue<String>("id");
}

void StorageElasticsearchQueue::closePointInTime(const String & pit_id) const
{
    Poco::JSON::Object::Ptr body = new Poco::JSON::Object;
    body->set("id", pit_id);

    try
    {
        executeHTTPRequest(Poco::Net::HTTPRequest::HTTP_DELETE, Poco::URI(makeEndpointURL(url, "_pit")), serializeJSONObject(body));
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to close Elasticsearch point-in-time context");
    }
}

Poco::JSON::Object::Ptr StorageElasticsearchQueue::makeSearchBody(size_t max_rows, const String & search_after, const String & pit_id) const
{
    Poco::JSON::Object::Ptr body = new Poco::JSON::Object;

    if (!query_body.empty())
    {
        Poco::JSON::Parser parser;
        auto parsed = parser.parse(query_body);
        if (parsed.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "elasticsearch_query must be a JSON object");
        body = parsed.extract<Poco::JSON::Object::Ptr>();
    }
    else
    {
        Poco::JSON::Object::Ptr match_all = new Poco::JSON::Object;
        match_all->set("match_all", Poco::JSON::Object::Ptr(new Poco::JSON::Object));
        body->set("query", match_all);
    }

    /// The engine owns cursor and PIT state. Values supplied by a user query would bypass checkpoints.
    body->remove("pit");
    body->remove("search_after");

    if (!pit_id.empty())
    {
        Poco::JSON::Object::Ptr pit = new Poco::JSON::Object;
        pit->set("id", pit_id);
        pit->set("keep_alive", pit_keep_alive);
        body->set("pit", pit);
    }

    body->set("size", static_cast<UInt64>(max_rows));

    Poco::JSON::Array::Ptr sort = new Poco::JSON::Array;
    sort->add(makeAscendingSortEntry(cursor_field));
    if (!tiebreaker_field.empty() && tiebreaker_field != cursor_field)
        sort->add(makeAscendingSortEntry(tiebreaker_field));
    body->set("sort", sort);

    if (!search_after.empty())
    {
        Poco::JSON::Parser parser;
        auto parsed = parser.parse(search_after);
        if (parsed.type() != typeid(Poco::JSON::Array::Ptr))
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Stored ElasticsearchQueue checkpoint is not a JSON array: {}", search_after);
        body->set("search_after", parsed.extract<Poco::JSON::Array::Ptr>());
    }

    return body;
}

String StorageElasticsearchQueue::executeSearchRequest(size_t max_rows, const String & search_after) const
{
    String pit_id;
    String response;

    try
    {
        if (use_point_in_time)
            pit_id = openPointInTime();

        auto body = makeSearchBody(max_rows, search_after, pit_id);
        const String body_string = serializeJSONObject(body);
        const String endpoint = pit_id.empty() ? makeSearchURL(url, index) : makeEndpointURL(url, "_search");
        response = executeHTTPRequest(Poco::Net::HTTPRequest::HTTP_POST, Poco::URI(endpoint), body_string);

        if (!pit_id.empty())
        {
            pit_id = getUpdatedPointInTimeId(response, pit_id);
            closePointInTime(pit_id);
            pit_id.clear();
        }
    }
    catch (...)
    {
        if (!pit_id.empty())
            closePointInTime(pit_id);
        throw;
    }

    return response;
}

StorageElasticsearchQueue::Batch StorageElasticsearchQueue::pollBatch(size_t max_rows)
{
    std::lock_guard lock(mutex);

    if (shutdown_called)
        throw Exception(ErrorCodes::ABORTED, "Table is detached");

    Batch result;
    if (!keeper_path.empty())
    {
        auto [zookeeper, lock_holder] = tryAcquireKeeperCheckpointLock();
        if (!lock_holder)
            return result;

        result.checkpoint_zookeeper = std::move(zookeeper);
        result.checkpoint_lock = std::move(lock_holder);
        checkpoint = loadKeeperCheckpoint();
    }

    const String response = executeSearchRequest(max_rows, checkpoint);

    Poco::JSON::Parser parser;
    auto parsed = parser.parse(response);
    if (parsed.type() != typeid(Poco::JSON::Object::Ptr))
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Elasticsearch response must be a JSON object");

    auto root = parsed.extract<Poco::JSON::Object::Ptr>();
    auto hits_object = root->getObject("hits");
    if (!hits_object)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Elasticsearch response does not contain 'hits' object");

    auto hits = hits_object->getArray("hits");
    if (!hits || hits->size() == 0)
    {
        result.checkpoint_lock.reset();
        result.checkpoint_zookeeper.reset();
        return result;
    }

    result.rows.reserve(hits->size());
    for (size_t i = 0; i < hits->size(); ++i)
    {
        auto hit = hits->getObject(static_cast<UInt32>(i));
        if (!hit)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Elasticsearch response hit #{} is not an object", i);

        Poco::JSON::Object::Ptr source;
        if (hit->has("_source"))
        {
            auto source_value = hit->get("_source");
            if (source_value.type() == typeid(Poco::JSON::Object::Ptr))
                source = source_value.extract<Poco::JSON::Object::Ptr>();
        }

        if (!hit->has("sort"))
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Elasticsearch response hit #{} does not contain 'sort' cursor", i);

        result.checkpoint = serializeJSON(hit->get("sort"));
        result.rows.push_back(Row{.hit = hit, .source = source});
    }

    return result;
}

void StorageElasticsearchQueue::commit(
    const String & new_checkpoint,
    const zkutil::EphemeralNodeHolder::Ptr & checkpoint_lock,
    const zkutil::ZooKeeperPtr & checkpoint_zookeeper)
{
    std::lock_guard lock(mutex);

    if (new_checkpoint.empty() || new_checkpoint == checkpoint)
        return;

    if (!keeper_path.empty())
    {
        if (!checkpoint_lock || !checkpoint_zookeeper)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ElasticsearchQueue Keeper checkpoint must be committed while holding the Keeper lock");
        if (checkpoint_zookeeper->expired())
            throw Exception(ErrorCodes::NO_ZOOKEEPER, "Cannot commit ElasticsearchQueue Keeper checkpoint because Keeper session has expired");

        storeKeeperCheckpoint(new_checkpoint, checkpoint_zookeeper);
    }
    else
    {
        storeCheckpoint(new_checkpoint);
    }

    checkpoint = new_checkpoint;
}

size_t StorageElasticsearchQueue::getMaxBlockSize() const
{
    return (*settings)[ElasticsearchQueueSetting::elasticsearch_max_block_size].changed
        ? (*settings)[ElasticsearchQueueSetting::elasticsearch_max_block_size].value
        : getContext()->getSettingsRef()[Setting::max_insert_block_size].value;
}

size_t StorageElasticsearchQueue::getPollMaxBatchSize() const
{
    size_t batch_size = (*settings)[ElasticsearchQueueSetting::elasticsearch_poll_max_batch_size].changed
        ? (*settings)[ElasticsearchQueueSetting::elasticsearch_poll_max_batch_size].value
        : getContext()->getSettingsRef()[Setting::max_block_size].value;

    return std::min(batch_size, getMaxBlockSize());
}

size_t StorageElasticsearchQueue::getFlushIntervalMilliseconds() const
{
    const auto & flush_interval = (*settings)[ElasticsearchQueueSetting::elasticsearch_flush_interval_ms];
    return flush_interval.changed && flush_interval.totalMilliseconds() > 0
        ? flush_interval.totalMilliseconds()
        : getContext()->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds();
}

size_t StorageElasticsearchQueue::getConsumerRescheduleMilliseconds() const
{
    return (*settings)[ElasticsearchQueueSetting::elasticsearch_consumer_reschedule_ms].totalMilliseconds();
}

bool StorageElasticsearchQueue::commitOnSelect() const
{
    return (*settings)[ElasticsearchQueueSetting::elasticsearch_commit_on_select].value;
}

bool StorageElasticsearchQueue::checkDependencies(const StorageID & table_id) const
{
    return !DatabaseCatalog::instance().getReadyDependentViews(table_id, getContext()).empty();
}

size_t StorageElasticsearchQueue::getTableDependentCount() const
{
    return DatabaseCatalog::instance().getDependentViews(getStorageID()).size();
}

void StorageElasticsearchQueue::threadFunc()
{
    try
    {
        auto table_id = getStorageID();
        auto dependencies_count = getTableDependentCount();

        if (dependencies_count)
        {
            auto start_time = std::chrono::steady_clock::now();
            mv_attached.store(true);

            while (!task->stream_cancelled)
            {
                if (!checkDependencies(table_id))
                    break;

                LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

                if (streamToViews())
                    break;

                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts - start_time);
                if (duration > std::chrono::milliseconds(getFlushIntervalMilliseconds()))
                    break;
            }
        }
        else
        {
            LOG_DEBUG(log, "No attached views");
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    mv_attached.store(false);

    if (!task->stream_cancelled)
        task->holder->scheduleAfter(getConsumerRescheduleMilliseconds());
}

bool StorageElasticsearchQueue::streamToViews()
{
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(getContext(), false), getContext());

    auto insert = make_intrusive<ASTInsertQuery>();
    insert->table_id = table_id;

    auto elasticsearch_context = Context::createCopy(getContext());
    elasticsearch_context->makeQueryContext();

    InterpreterInsertQuery interpreter(
        insert,
        elasticsearch_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_insert */ false);

    auto block_io = interpreter.execute();

    auto source = std::make_shared<ElasticsearchQueueSource>(
        *this,
        storage_snapshot,
        elasticsearch_context,
        block_io.pipeline.getHeader().getNames(),
        getPollMaxBatchSize(),
        false);

    Pipe pipe(source);
    std::atomic_size_t rows = 0;
    {
        block_io.pipeline.complete(std::move(pipe));
        block_io.pipeline.setNumThreads(1);
        block_io.pipeline.setConcurrencyControl(elasticsearch_context->getSettingsRef()[Setting::use_concurrency_control]);
        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }

    source->commit();

    LOG_DEBUG(log, "Pushed {} rows to {}", rows.load(), table_id.getNameForLogs());
    return source->isStalled();
}

}
