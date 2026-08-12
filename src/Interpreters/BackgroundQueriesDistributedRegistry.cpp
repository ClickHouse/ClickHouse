#include <Interpreters/BackgroundQueriesDistributedRegistry.h>

#include <Core/ServerSettings.h>
#include <Core/UUID.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/KeeperFeatureFlags.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <base/getFQDNOrHostName.h>


#include <optional>
#include <unordered_set>

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsString background_queries_registry_zookeeper_path;
    extern const ServerSettingsUInt64 background_queries_registry_entry_ttl;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_TEXT;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

using Status = BackgroundQueriesDistributedRegistry::Status;

constexpr size_t ENTRY_ASYNCHRONOUS_UPDATE_QUEUE_SIZE = 100000;
constexpr size_t ENTRIES_BATCH_SIZE = 100;
constexpr UInt64 IDLE_TICK_PERIOD_MS = 10'000;

}

String BackgroundQueriesDistributedRegistry::Entry::toString() const
{
    WriteBufferFromOwnString out;
    out << "version: 1\n";
    out << "query_id: ";
    writeEscapedString(query_id, out);
    out << "\nhost: ";
    writeEscapedString(host, out);
    out << "\nuser: ";
    writeEscapedString(user, out);
    out << "\nincarnation_id: ";
    writeEscapedString(incarnation_id, out);
    out << "\nstatus: " << static_cast<int>(status);
    out << "\nexception_code: " << exception_code;
    out << "\nexception: ";
    writeEscapedString(exception, out);
    out << "\nquery: ";
    writeEscapedString(query, out);
    out << "\nsubmit_time: " << submit_time;
    out << "\nfinish_time: " << finish_time;
    out << "\n";
    return out.str();
}

BackgroundQueriesDistributedRegistry::Entry
BackgroundQueriesDistributedRegistry::Entry::parse(const String & data)
{
    Entry entry{};
    ReadBufferFromString in(data);
    UInt64 version = 0;
    in >> "version: " >> version >> "\n";
    if (version < 1)
        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Unexpected background query entry version {}", version);
    in >> "query_id: ";
    readEscapedString(entry.query_id, in);
    in >> "\nhost: ";
    readEscapedString(entry.host, in);
    in >> "\nuser: ";
    readEscapedString(entry.user, in);
    in >> "\nincarnation_id: ";
    readEscapedString(entry.incarnation_id, in);
    int status = 0;
    in >> "\nstatus: " >> status;
    if (status < static_cast<int>(Status::Running) || status > static_cast<int>(Status::InternalRegistryError))
        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Unexpected background query status {}", status);
    entry.status = static_cast<Status>(status);
    in >> "\nexception_code: " >> entry.exception_code;
    in >> "\nexception: ";
    readEscapedString(entry.exception, in);
    in >> "\nquery: ";
    readEscapedString(entry.query, in);
    in >> "\nsubmit_time: " >> entry.submit_time;
    in >> "\nfinish_time: " >> entry.finish_time;

    return entry;
}

void BackgroundQueryHandle::onFinish()
{
    auto registry_ptr = registry.lock();
    if (!registry_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The background queries registry was destroyed before a background query finished");

    registry_ptr->finalizeQuery(*this, Status::Finished, 0, "");
}

void BackgroundQueryHandle::onException(int code, const String & message)
{
    auto registry_ptr = registry.lock();
    if (!registry_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The background queries registry was destroyed before a background query finished");

    registry_ptr->finalizeQuery(*this, Status::Failed, code, message);
}

BackgroundQueriesDistributedRegistry::BackgroundQueriesDistributedRegistry(ContextPtr global_context_)
    : global_context(global_context_)
    , log(getLogger("BackgroundQueriesDistributedRegistry"))
    , host(getFQDNOrHostName())
    , incarnation_id(host + "-" + toString(UUIDHelpers::generateV4()))
    , zookeeper_path(global_context_->getServerSettings()[ServerSetting::background_queries_registry_zookeeper_path])
    , entries_path(zookeeper_path + "/entries")
    , entry_path_prefix(entries_path + "/query-")
    , incarnations_path(zookeeper_path + "/incarnations")
    , incarnation_path(incarnations_path + "/" + incarnation_id)
    , entry_ttl_ms(global_context_->getServerSettings()[ServerSetting::background_queries_registry_entry_ttl] * 1000)
    , entry_asynchronous_update_queue(ENTRY_ASYNCHRONOUS_UPDATE_QUEUE_SIZE)
{
    if (zookeeper_path.ends_with('/'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`background_queries_registry_zookeeper_path` must not end with '/'");

    thread = ThreadFromGlobalPool([this] { threadFunction(); });
}

BackgroundQueriesDistributedRegistry::~BackgroundQueriesDistributedRegistry()
{
    shutdown();
}

zkutil::ZooKeeperPtr BackgroundQueriesDistributedRegistry::getZooKeeper() const
{
    auto zookeeper = global_context->getZooKeeper();
    if (!zookeeper->isFeatureEnabled(KeeperFeatureFlag::CREATE_TTL)
        || !zookeeper->isFeatureEnabled(KeeperFeatureFlag::LIST_WITH_STAT_AND_DATA))
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "The background queries registry requires a [Zoo]Keeper server that supports the `CREATE_TTL` "
            "and `LIST_WITH_STAT_AND_DATA` feature flags, and they must be enabled");
    return zookeeper;
}

void BackgroundQueriesDistributedRegistry::ensureIncarnationNode(const zkutil::ZooKeeperPtr & zookeeper)
{
    auto code = zookeeper->tryCreate(incarnation_path, host, zkutil::CreateMode::Ephemeral);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        throw zkutil::KeeperException::fromPath(code, incarnation_path);
}

BackgroundQueryHandlePtr BackgroundQueriesDistributedRegistry::registerQuery(const String & query_id, const String & user, const String & query)
{
    auto component_guard = Coordination::setCurrentComponent("BackgroundQueriesDistributedRegistry::registerQuery");
    auto zookeeper = getZooKeeper();

    zookeeper->createAncestors(entries_path);
    zookeeper->createIfNotExists(entries_path, "");
    zookeeper->createIfNotExists(incarnations_path, "");
    ensureIncarnationNode(zookeeper);

    Entry entry{
        .query_id = query_id,
        .host = host,
        .user = user,
        .incarnation_id = incarnation_id,
        .query = query,
        .status = Status::Running,
        .exception_code = 0,
        .exception = "",
        .submit_time = time(nullptr),
    };

    String entry_path;
    auto code = zookeeper->tryCreate(entry_path_prefix, entry.toString(), zkutil::CreateMode::PersistentSequential, entry_path, entry_ttl_ms);
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, entry_path_prefix);

    return BackgroundQueryHandlePtr(new BackgroundQueryHandle(weak_from_this(), std::move(entry_path), std::move(entry)));
}

void BackgroundQueriesDistributedRegistry::finalizeQuery(BackgroundQueryHandle & handle, Status status, Int32 exception_code, const String & exception)
{
    handle.entry.status = status;
    handle.entry.exception_code = exception_code;
    handle.entry.exception = exception;
    handle.entry.finish_time = time(nullptr);

    if (!entry_asynchronous_update_queue.push(EntryUpdate{handle.entry_path, handle.entry}))
        LOG_WARNING(log, "Dropping the outcome of background query {}: the registry is shut down", handle.entry.query_id);
}

void BackgroundQueriesDistributedRegistry::threadFunction()
{
    auto component_guard = Coordination::setCurrentComponent("BackgroundQueriesDistributedRegistry::run");

    /// We may pop an entry from the queue and fail to process it due to (intermittent) ZooKeeper errors.
    /// Stash it to pending_update to avoid having to push it to the queue again.
    std::optional<EntryUpdate> pending_update;

    while (true)
    {
        if (!pending_update)
        {
            EntryUpdate update{};
            if (entry_asynchronous_update_queue.tryPop(update, IDLE_TICK_PERIOD_MS))
                pending_update = std::move(update);
        }

        if (pending_update)
        {
            auto & update = *pending_update;
            try
            {
                auto zookeeper = getZooKeeper();
                const auto body = update.entry.toString();
                auto code = zookeeper->trySet(update.entry_path, body);
                if (code == Coordination::Error::ZNONODE)
                    code = zookeeper->tryCreate(update.entry_path, body, zkutil::CreateMode::Persistent, entry_ttl_ms);
                if (code != Coordination::Error::ZOK)
                    throw zkutil::KeeperException::fromPath(code, update.entry_path);
                pending_update.reset();
            }
            catch (...)
            {
                tryLogCurrentException(log);
                if (entry_asynchronous_update_queue.isFinished())
                {
                    LOG_WARNING(log, "Dropping the outcome of background query {}: the registry is shutting down", update.entry.query_id);
                    pending_update.reset();
                }
            }
        }

        if (entry_asynchronous_update_queue.isFinishedAndEmpty())
            return;

        try
        {
            /// If the session has changed, we need to recreate our ephemeral incarnation node.
            if (auto zookeeper = getZooKeeper(); zookeeper != incarnation_node_session)
            {
                ensureIncarnationNode(zookeeper);
                incarnation_node_session = zookeeper;
            }
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
}

void BackgroundQueriesDistributedRegistry::forEach(const std::function<void(Entry)> & callback)
{
    auto component_guard = Coordination::setCurrentComponent("BackgroundQueriesDistributedRegistry::forEach");
    auto zookeeper = getZooKeeper();

    auto responses = zookeeper->tryGetChildren({entries_path, incarnations_path});

    auto & entries_response = responses[0];
    auto & incarnations_response = responses[1];
    if (entries_response.error == Coordination::Error::ZNONODE)
        return;

    std::unordered_set<std::string_view> incarnations;
    if (incarnations_response.error == Coordination::Error::ZOK)
        incarnations.insert(incarnations_response.names.begin(), incarnations_response.names.end());

    std::vector<String> batch;
    batch.reserve(std::min(entries_response.names.size(), ENTRIES_BATCH_SIZE));

    for (size_t batch_begin = 0; batch_begin < entries_response.names.size(); batch_begin += ENTRIES_BATCH_SIZE)
    {
        const size_t batch_end = std::min(batch_begin + ENTRIES_BATCH_SIZE, entries_response.names.size());

        batch.clear();
        for (size_t i = batch_begin; i < batch_end; ++i)
            batch.push_back(entries_path + "/" + entries_response.names[i]);

        auto entry_responses = zookeeper->tryGet(batch);
        for (size_t i = 0; i < batch.size(); ++i)
        {
            const auto & entry_response = entry_responses[i];
            if (entry_response.error == Coordination::Error::ZNONODE)
                continue;

            Entry entry{};
            try
            {
                if (entry_response.error != Coordination::Error::ZOK)
                    throw zkutil::KeeperException::fromPath(entry_response.error, batch[i]);
                entry = Entry::parse(entry_response.data);

                /// If an entry has a "Running" status but an expired replica incarnation, override the status to "Unknown".
                /// Otherwise, queries on crashed or lost (i.e. no keeper connection for a long time followed by restart) replicas
                /// would have a "Running" status until their znodes expire.
                if (entry.status == Status::Running && !incarnations.contains(entry.incarnation_id))
                    entry.status = Status::Unknown;
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Cannot read the background query entry at {}", batch[i]));
                entry = Entry{};
                entry.status = Status::InternalRegistryError;
            }

            callback(std::move(entry));
        }
    }
}

void BackgroundQueriesDistributedRegistry::truncate()
{
    auto component_guard = Coordination::setCurrentComponent("BackgroundQueriesDistributedRegistry::truncate");
    auto zookeeper = getZooKeeper();
    zookeeper->tryRemoveChildrenRecursive(entries_path, /*probably_flat=*/ true);
}

void BackgroundQueriesDistributedRegistry::shutdown()
{
    if (entry_asynchronous_update_queue.finish())
        return;

    if (thread.joinable())
        thread.join();
}

}
