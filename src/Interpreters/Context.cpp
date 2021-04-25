#include <map>
#include <set>
#include <optional>
#include <memory>
#include <Poco/Mutex.h>
#include <Poco/UUID.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Util/Application.h>
#include <Common/Macros.h>
#include <Common/escapeForFileName.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/thread_local_rng.h>
#include <Coordination/KeeperStorageDispatcher.h>
#include <Compression/ICompressionCodec.h>
#include <Core/BackgroundSchedulePool.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/CompressionCodecSelector.h>
#include <Storages/StorageS3Settings.h>
#include <Disks/DiskLocal.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/ExternalLoaderXMLConfigRepository.h>
#include <Core/Settings.h>
#include <Core/SettingsQuirks.h>
#include <Access/AccessControlManager.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/QuotaUsage.h>
#include <Access/User.h>
#include <Access/Credentials.h>
#include <Access/SettingsProfile.h>
#include <Access/SettingsConstraints.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/GSSAcceptor.h>
#include <Interpreters/ExpressionJIT.h>
#include <Dictionaries/Embedded/GeoDictionariesLoader.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/InterserverCredentials.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DDLTask.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/UncompressedCache.h>
#include <IO/MMappedFileCache.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/StackTrace.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ShellCommand.h>
#include <Common/TraceCollector.h>
#include <common/logger_useful.h>
#include <Common/RemoteHostFilter.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>


namespace ProfileEvents
{
    extern const Event ContextLock;
    extern const Event CompiledCacheSizeBytes;
}

namespace CurrentMetrics
{
    extern const Metric ContextLockWait;
    extern const Metric BackgroundMovePoolTask;
    extern const Metric BackgroundSchedulePoolTask;
    extern const Metric BackgroundBufferFlushSchedulePoolTask;
    extern const Metric BackgroundDistributedSchedulePoolTask;
    extern const Metric BackgroundMessageBrokerSchedulePoolTask;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int BAD_GET;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int THERE_IS_NO_SESSION;
    extern const int THERE_IS_NO_QUERY;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT;
    extern const int SESSION_NOT_FOUND;
    extern const int SESSION_IS_LOCKED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


class NamedSessions
{
public:
    using Key = NamedSessionKey;

    ~NamedSessions()
    {
        try
        {
            {
                std::lock_guard lock{mutex};
                quit = true;
            }

            cond.notify_one();
            thread.join();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    /// Find existing session or create a new.
    std::shared_ptr<NamedSession> acquireSession(
        const String & session_id,
        ContextPtr context,
        std::chrono::steady_clock::duration timeout,
        bool throw_if_not_found)
    {
        std::unique_lock lock(mutex);

        auto & user_name = context->client_info.current_user;

        if (user_name.empty())
            throw Exception("Empty user name.", ErrorCodes::LOGICAL_ERROR);

        Key key(user_name, session_id);

        auto it = sessions.find(key);
        if (it == sessions.end())
        {
            if (throw_if_not_found)
                throw Exception("Session not found.", ErrorCodes::SESSION_NOT_FOUND);

            /// Create a new session from current context.
            it = sessions.insert(std::make_pair(key, std::make_shared<NamedSession>(key, context, timeout, *this))).first;
        }
        else if (it->second->key.first != context->client_info.current_user)
        {
            throw Exception("Session belongs to a different user", ErrorCodes::SESSION_IS_LOCKED);
        }

        /// Use existing session.
        const auto & session = it->second;

        if (!session.unique())
            throw Exception("Session is locked by a concurrent client.", ErrorCodes::SESSION_IS_LOCKED);

        session->context->client_info = context->client_info;

        return session;
    }

    void releaseSession(NamedSession & session)
    {
        std::unique_lock lock(mutex);
        scheduleCloseSession(session, lock);
    }

private:
    class SessionKeyHash
    {
    public:
        size_t operator()(const Key & key) const
        {
            SipHash hash;
            hash.update(key.first);
            hash.update(key.second);
            return hash.get64();
        }
    };

    /// TODO it's very complicated. Make simple std::map with time_t or boost::multi_index.
    using Container = std::unordered_map<Key, std::shared_ptr<NamedSession>, SessionKeyHash>;
    using CloseTimes = std::deque<std::vector<Key>>;
    Container sessions;
    CloseTimes close_times;
    std::chrono::steady_clock::duration close_interval = std::chrono::seconds(1);
    std::chrono::steady_clock::time_point close_cycle_time = std::chrono::steady_clock::now();
    UInt64 close_cycle = 0;

    void scheduleCloseSession(NamedSession & session, std::unique_lock<std::mutex> &)
    {
        /// Push it on a queue of sessions to close, on a position corresponding to the timeout.
        /// (timeout is measured from current moment of time)

        const UInt64 close_index = session.timeout / close_interval + 1;
        const auto new_close_cycle = close_cycle + close_index;

        if (session.close_cycle != new_close_cycle)
        {
            session.close_cycle = new_close_cycle;
            if (close_times.size() < close_index + 1)
                close_times.resize(close_index + 1);
            close_times[close_index].emplace_back(session.key);
        }
    }

    void cleanThread()
    {
        setThreadName("SessionCleaner");
        std::unique_lock lock{mutex};

        while (true)
        {
            auto interval = closeSessions(lock);

            if (cond.wait_for(lock, interval, [this]() -> bool { return quit; }))
                break;
        }
    }

    /// Close sessions, that has been expired. Returns how long to wait for next session to be expired, if no new sessions will be added.
    std::chrono::steady_clock::duration closeSessions(std::unique_lock<std::mutex> & lock)
    {
        const auto now = std::chrono::steady_clock::now();

        /// The time to close the next session did not come
        if (now < close_cycle_time)
            return close_cycle_time - now;  /// Will sleep until it comes.

        const auto current_cycle = close_cycle;

        ++close_cycle;
        close_cycle_time = now + close_interval;

        if (close_times.empty())
            return close_interval;

        auto & sessions_to_close = close_times.front();

        for (const auto & key : sessions_to_close)
        {
            const auto session = sessions.find(key);

            if (session != sessions.end() && session->second->close_cycle <= current_cycle)
            {
                if (!session->second.unique())
                {
                    /// Skip but move it to close on the next cycle.
                    session->second->timeout = std::chrono::steady_clock::duration{0};
                    scheduleCloseSession(*session->second, lock);
                }
                else
                    sessions.erase(session);
            }
        }

        close_times.pop_front();
        return close_interval;
    }

    std::mutex mutex;
    std::condition_variable cond;
    std::atomic<bool> quit{false};
    ThreadFromGlobalPool thread{&NamedSessions::cleanThread, this};
};


void NamedSession::release()
{
    parent.releaseSession(*this);
}


/** Set of known objects (environment), that could be used in query.
  * Shared (global) part. Order of members (especially, order of destruction) is very important.
  */
struct ContextSharedPart
{
    Poco::Logger * log = &Poco::Logger::get("Context");

    /// For access of most of shared objects. Recursive mutex.
    mutable std::recursive_mutex mutex;
    /// Separate mutex for access of dictionaries. Separate mutex to avoid locks when server doing request to itself.
    mutable std::mutex embedded_dictionaries_mutex;
    mutable std::mutex external_dictionaries_mutex;
    mutable std::mutex external_models_mutex;
    /// Separate mutex for storage policies. During server startup we may
    /// initialize some important storages (system logs with MergeTree engine)
    /// under context lock.
    mutable std::mutex storage_policies_mutex;
    /// Separate mutex for re-initialization of zookeeper session. This operation could take a long time and must not interfere with another operations.
    mutable std::mutex zookeeper_mutex;

    mutable zkutil::ZooKeeperPtr zookeeper;                 /// Client for ZooKeeper.
    ConfigurationPtr zookeeper_config;                      /// Stores zookeeper configs

#if USE_NURAFT
    mutable std::mutex nu_keeper_storage_dispatcher_mutex;
    mutable std::shared_ptr<KeeperStorageDispatcher> nu_keeper_storage_dispatcher;
#endif
    mutable std::mutex auxiliary_zookeepers_mutex;
    mutable std::map<String, zkutil::ZooKeeperPtr> auxiliary_zookeepers;    /// Map for auxiliary ZooKeeper clients.
    ConfigurationPtr auxiliary_zookeepers_config;           /// Stores auxiliary zookeepers configs

    String interserver_io_host;                             /// The host name by which this server is available for other servers.
    UInt16 interserver_io_port = 0;                         /// and port.
    String interserver_scheme;                              /// http or https
    MultiVersion<InterserverCredentials> interserver_io_credentials;

    String path;                                            /// Path to the data directory, with a slash at the end.
    String flags_path;                                      /// Path to the directory with some control flags for server maintenance.
    String user_files_path;                                 /// Path to the directory with user provided files, usable by 'file' table function.
    String dictionaries_lib_path;                           /// Path to the directory with user provided binaries and libraries for external dictionaries.
    ConfigurationPtr config;                                /// Global configuration settings.

    String tmp_path;                                        /// Path to the temporary files that occur when processing the request.
    mutable VolumePtr tmp_volume;                           /// Volume for the the temporary files that occur when processing the request.

    mutable std::optional<EmbeddedDictionaries> embedded_dictionaries;    /// Metrica's dictionaries. Have lazy initialization.
    mutable std::optional<ExternalDictionariesLoader> external_dictionaries_loader;
    mutable std::optional<ExternalModelsLoader> external_models_loader;
    ConfigurationPtr external_models_config;
    ext::scope_guard models_repository_guard;

    String default_profile_name;                            /// Default profile name used for default values.
    String system_profile_name;                             /// Profile used by system processes
    String buffer_profile_name;                             /// Profile used by Buffer engine for flushing to the underlying
    AccessControlManager access_control_manager;
    mutable UncompressedCachePtr uncompressed_cache;        /// The cache of decompressed blocks.
    mutable MarkCachePtr mark_cache;                        /// Cache of marks in compressed files.
    mutable MMappedFileCachePtr mmap_cache; /// Cache of mmapped files to avoid frequent open/map/unmap/close and to reuse from several threads.
    ProcessList process_list;                               /// Executing queries at the moment.
    MergeList merge_list;                                   /// The list of executable merge (for (Replicated)?MergeTree)
    ReplicatedFetchList replicated_fetch_list;
    ConfigurationPtr users_config;                          /// Config with the users, profiles and quotas sections.
    InterserverIOHandler interserver_io_handler;            /// Handler for interserver communication.

    mutable std::optional<BackgroundSchedulePool> buffer_flush_schedule_pool; /// A thread pool that can do background flush for Buffer tables.
    mutable std::optional<BackgroundSchedulePool> schedule_pool;    /// A thread pool that can run different jobs in background (used in replicated tables)
    mutable std::optional<BackgroundSchedulePool> distributed_schedule_pool; /// A thread pool that can run different jobs in background (used for distributed sends)
    mutable std::optional<BackgroundSchedulePool> message_broker_schedule_pool; /// A thread pool that can run different jobs in background (used for message brokers, like RabbitMQ and Kafka)
    MultiVersion<Macros> macros;                            /// Substitutions extracted from config.
    std::unique_ptr<DDLWorker> ddl_worker;                  /// Process ddl commands from zk.
    /// Rules for selecting the compression settings, depending on the size of the part.
    mutable std::unique_ptr<CompressionCodecSelector> compression_codec_selector;
    /// Storage disk chooser for MergeTree engines
    mutable std::shared_ptr<const DiskSelector> merge_tree_disk_selector;
    /// Storage policy chooser for MergeTree engines
    mutable std::shared_ptr<const StoragePolicySelector> merge_tree_storage_policy_selector;

    std::optional<MergeTreeSettings> merge_tree_settings;   /// Settings of MergeTree* engines.
    std::optional<MergeTreeSettings> replicated_merge_tree_settings;   /// Settings of ReplicatedMergeTree* engines.
    std::atomic_size_t max_table_size_to_drop = 50000000000lu; /// Protects MergeTree tables from accidental DROP (50GB by default)
    std::atomic_size_t max_partition_size_to_drop = 50000000000lu; /// Protects MergeTree partitions from accidental DROP (50GB by default)
    String format_schema_path;                              /// Path to a directory that contains schema files used by input formats.
    ActionLocksManagerPtr action_locks_manager;             /// Set of storages' action lockers
    std::unique_ptr<SystemLogs> system_logs;                /// Used to log queries and operations on parts
    std::optional<StorageS3Settings> storage_s3_settings;   /// Settings of S3 storage

    RemoteHostFilter remote_host_filter; /// Allowed URL from config.xml

    std::optional<TraceCollector> trace_collector;        /// Thread collecting traces from threads executing queries
    std::optional<NamedSessions> named_sessions;        /// Controls named HTTP sessions.

    /// Clusters for distributed tables
    /// Initialized on demand (on distributed storages initialization) since Settings should be initialized
    std::unique_ptr<Clusters> clusters;
    ConfigurationPtr clusters_config;                        /// Stores updated configs
    mutable std::mutex clusters_mutex;                       /// Guards clusters and clusters_config

    bool shutdown_called = false;

    Stopwatch uptime_watch;

    Context::ApplicationType application_type = Context::ApplicationType::SERVER;

    /// vector of xdbc-bridge commands, they will be killed when Context will be destroyed
    std::vector<std::unique_ptr<ShellCommand>> bridge_commands;

    Context::ConfigReloadCallback config_reload_callback;

    ContextSharedPart()
        : macros(std::make_unique<Macros>())
    {
        /// TODO: make it singleton (?)
        static std::atomic<size_t> num_calls{0};
        if (++num_calls > 1)
        {
            std::cerr << "Attempting to create multiple ContextShared instances. Stack trace:\n" << StackTrace().toString();
            std::cerr.flush();
            std::terminate();
        }
    }


    ~ContextSharedPart()
    {
        try
        {
            shutdown();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }


    /** Perform a complex job of destroying objects in advance.
      */
    void shutdown()
    {
        if (shutdown_called)
            return;
        shutdown_called = true;

        /**  After system_logs have been shut down it is guaranteed that no system table gets created or written to.
          *  Note that part changes at shutdown won't be logged to part log.
          */

        if (system_logs)
            system_logs->shutdown();

        DatabaseCatalog::shutdown();

        std::unique_ptr<SystemLogs> delete_system_logs;
        {
            auto lock = std::lock_guard(mutex);

            /// Preemptive destruction is important, because these objects may have a refcount to ContextShared (cyclic reference).
            /// TODO: Get rid of this.

            delete_system_logs = std::move(system_logs);
            embedded_dictionaries.reset();
            external_dictionaries_loader.reset();
            models_repository_guard.reset();
            external_models_loader.reset();
            buffer_flush_schedule_pool.reset();
            schedule_pool.reset();
            distributed_schedule_pool.reset();
            message_broker_schedule_pool.reset();
            ddl_worker.reset();

            /// Stop trace collector if any
            trace_collector.reset();
            /// Stop zookeeper connection
            zookeeper.reset();
        }

        /// Can be removed w/o context lock
        delete_system_logs.reset();
    }

    bool hasTraceCollector() const
    {
        return trace_collector.has_value();
    }

    void initializeTraceCollector(std::shared_ptr<TraceLog> trace_log)
    {
        if (!trace_log)
            return;
        if (hasTraceCollector())
            return;

        trace_collector.emplace(std::move(trace_log));
    }
};


Context::Context() = default;
Context::Context(const Context &) = default;
Context & Context::operator=(const Context &) = default;

SharedContextHolder::SharedContextHolder(SharedContextHolder &&) noexcept = default;
SharedContextHolder & SharedContextHolder::operator=(SharedContextHolder &&) = default;
SharedContextHolder::SharedContextHolder() = default;
SharedContextHolder::~SharedContextHolder() = default;
SharedContextHolder::SharedContextHolder(std::unique_ptr<ContextSharedPart> shared_context)
    : shared(std::move(shared_context)) {}

void SharedContextHolder::reset() { shared.reset(); }

ContextPtr Context::createGlobal(ContextSharedPart * shared)
{
    auto res = std::shared_ptr<Context>(new Context);
    res->shared = shared;
    return res;
}

void Context::initGlobal()
{
    DatabaseCatalog::init(shared_from_this());
}

SharedContextHolder Context::createShared()
{
    return SharedContextHolder(std::make_unique<ContextSharedPart>());
}

ContextPtr Context::createCopy(const ContextConstPtr & other)
{
    return std::shared_ptr<Context>(new Context(*other));
}

ContextPtr Context::createCopy(const ContextWeakConstPtr & other)
{
    auto ptr = other.lock();
    if (!ptr) throw Exception("Can't copy an expired context", ErrorCodes::LOGICAL_ERROR);
    return createCopy(ptr);
}

ContextPtr Context::createCopy(const ContextPtr & other)
{
    return createCopy(std::const_pointer_cast<const Context>(other));
}

void Context::copyFrom(const ContextPtr & other)
{
    *this = *other;
}

Context::~Context() = default;


InterserverIOHandler & Context::getInterserverIOHandler() { return shared->interserver_io_handler; }

std::unique_lock<std::recursive_mutex> Context::getLock() const
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    return std::unique_lock(shared->mutex);
}

ProcessList & Context::getProcessList() { return shared->process_list; }
const ProcessList & Context::getProcessList() const { return shared->process_list; }
MergeList & Context::getMergeList() { return shared->merge_list; }
const MergeList & Context::getMergeList() const { return shared->merge_list; }
ReplicatedFetchList & Context::getReplicatedFetchList() { return shared->replicated_fetch_list; }
const ReplicatedFetchList & Context::getReplicatedFetchList() const { return shared->replicated_fetch_list; }


void Context::enableNamedSessions()
{
    shared->named_sessions.emplace();
}

std::shared_ptr<NamedSession>
Context::acquireNamedSession(const String & session_id, std::chrono::steady_clock::duration timeout, bool session_check)
{
    if (!shared->named_sessions)
        throw Exception("Support for named sessions is not enabled", ErrorCodes::NOT_IMPLEMENTED);

    return shared->named_sessions->acquireSession(session_id, shared_from_this(), timeout, session_check);
}

String Context::resolveDatabase(const String & database_name) const
{
    String res = database_name.empty() ? getCurrentDatabase() : database_name;
    if (res.empty())
        throw Exception("Default database is not selected", ErrorCodes::UNKNOWN_DATABASE);
    return res;
}

String Context::getPath() const
{
    auto lock = getLock();
    return shared->path;
}

String Context::getFlagsPath() const
{
    auto lock = getLock();
    return shared->flags_path;
}

String Context::getUserFilesPath() const
{
    auto lock = getLock();
    return shared->user_files_path;
}

String Context::getDictionariesLibPath() const
{
    auto lock = getLock();
    return shared->dictionaries_lib_path;
}

VolumePtr Context::getTemporaryVolume() const
{
    auto lock = getLock();
    return shared->tmp_volume;
}

void Context::setPath(const String & path)
{
    auto lock = getLock();

    shared->path = path;

    if (shared->tmp_path.empty() && !shared->tmp_volume)
        shared->tmp_path = shared->path + "tmp/";

    if (shared->flags_path.empty())
        shared->flags_path = shared->path + "flags/";

    if (shared->user_files_path.empty())
        shared->user_files_path = shared->path + "user_files/";

    if (shared->dictionaries_lib_path.empty())
        shared->dictionaries_lib_path = shared->path + "dictionaries_lib/";
}

VolumePtr Context::setTemporaryStorage(const String & path, const String & policy_name)
{
    std::lock_guard lock(shared->storage_policies_mutex);

    if (policy_name.empty())
    {
        shared->tmp_path = path;
        if (!shared->tmp_path.ends_with('/'))
            shared->tmp_path += '/';

        auto disk = std::make_shared<DiskLocal>("_tmp_default", shared->tmp_path, 0);
        shared->tmp_volume = std::make_shared<SingleDiskVolume>("_tmp_default", disk, 0);
    }
    else
    {
        StoragePolicyPtr tmp_policy = getStoragePolicySelector(lock)->get(policy_name);
        if (tmp_policy->getVolumes().size() != 1)
             throw Exception("Policy " + policy_name + " is used temporary files, such policy should have exactly one volume",
                             ErrorCodes::NO_ELEMENTS_IN_CONFIG);
        shared->tmp_volume = tmp_policy->getVolume(0);
    }

    if (shared->tmp_volume->getDisks().empty())
         throw Exception("No disks volume for temporary files", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    return shared->tmp_volume;
}

void Context::setFlagsPath(const String & path)
{
    auto lock = getLock();
    shared->flags_path = path;
}

void Context::setUserFilesPath(const String & path)
{
    auto lock = getLock();
    shared->user_files_path = path;
}

void Context::setDictionariesLibPath(const String & path)
{
    auto lock = getLock();
    shared->dictionaries_lib_path = path;
}

void Context::setConfig(const ConfigurationPtr & config)
{
    auto lock = getLock();
    shared->config = config;
    shared->access_control_manager.setExternalAuthenticatorsConfig(*shared->config);
}

const Poco::Util::AbstractConfiguration & Context::getConfigRef() const
{
    auto lock = getLock();
    return shared->config ? *shared->config : Poco::Util::Application::instance().config();
}


AccessControlManager & Context::getAccessControlManager()
{
    return shared->access_control_manager;
}

const AccessControlManager & Context::getAccessControlManager() const
{
    return shared->access_control_manager;
}

void Context::setExternalAuthenticatorsConfig(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = getLock();
    shared->access_control_manager.setExternalAuthenticatorsConfig(config);
}

std::unique_ptr<GSSAcceptorContext> Context::makeGSSAcceptorContext() const
{
    auto lock = getLock();
    return std::make_unique<GSSAcceptorContext>(shared->access_control_manager.getExternalAuthenticators().getKerberosParams());
}

void Context::setUsersConfig(const ConfigurationPtr & config)
{
    auto lock = getLock();
    shared->users_config = config;
    shared->access_control_manager.setUsersConfig(*shared->users_config);
}

ConfigurationPtr Context::getUsersConfig()
{
    auto lock = getLock();
    return shared->users_config;
}


void Context::setUser(const Credentials & credentials, const Poco::Net::SocketAddress & address)
{
    auto lock = getLock();

    client_info.current_user = credentials.getUserName();
    client_info.current_address = address;

#if defined(ARCADIA_BUILD)
    /// This is harmful field that is used only in foreign "Arcadia" build.
    client_info.current_password.clear();
    if (const auto * basic_credentials = dynamic_cast<const BasicCredentials *>(&credentials))
        client_info.current_password = basic_credentials->getPassword();
#endif

    /// Find a user with such name and check the credentials.
    auto new_user_id = getAccessControlManager().login(credentials, address.host());
    auto new_access = getAccessControlManager().getContextAccess(
        new_user_id, /* current_roles = */ {}, /* use_default_roles = */ true,
        settings, current_database, client_info);

    user_id = new_user_id;
    access = std::move(new_access);
    current_roles.clear();
    use_default_roles = true;

    setSettings(*access->getDefaultSettings());
}

void Context::setUser(const String & name, const String & password, const Poco::Net::SocketAddress & address)
{
    setUser(BasicCredentials(name, password), address);
}

void Context::setUserWithoutCheckingPassword(const String & name, const Poco::Net::SocketAddress & address)
{
    setUser(AlwaysAllowCredentials(name), address);
}

std::shared_ptr<const User> Context::getUser() const
{
    return getAccess()->getUser();
}

void Context::setQuotaKey(String quota_key_)
{
    auto lock = getLock();
    client_info.quota_key = std::move(quota_key_);
}

String Context::getUserName() const
{
    return getAccess()->getUserName();
}

std::optional<UUID> Context::getUserID() const
{
    auto lock = getLock();
    return user_id;
}


void Context::setCurrentRoles(const std::vector<UUID> & current_roles_)
{
    auto lock = getLock();
    if (current_roles == current_roles_ && !use_default_roles)
        return;
    current_roles = current_roles_;
    use_default_roles = false;
    calculateAccessRights();
}

void Context::setCurrentRolesDefault()
{
    auto lock = getLock();
    if (use_default_roles)
        return;
    current_roles.clear();
    use_default_roles = true;
    calculateAccessRights();
}

boost::container::flat_set<UUID> Context::getCurrentRoles() const
{
    return getRolesInfo()->current_roles;
}

boost::container::flat_set<UUID> Context::getEnabledRoles() const
{
    return getRolesInfo()->enabled_roles;
}

std::shared_ptr<const EnabledRolesInfo> Context::getRolesInfo() const
{
    return getAccess()->getRolesInfo();
}


void Context::calculateAccessRights()
{
    auto lock = getLock();
    if (user_id)
        access = getAccessControlManager().getContextAccess(*user_id, current_roles, use_default_roles, settings, current_database, client_info);
}


template <typename... Args>
void Context::checkAccessImpl(const Args &... args) const
{
    return getAccess()->checkAccess(args...);
}

void Context::checkAccess(const AccessFlags & flags) const { return checkAccessImpl(flags); }
void Context::checkAccess(const AccessFlags & flags, const std::string_view & database) const { return checkAccessImpl(flags, database); }
void Context::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table) const { return checkAccessImpl(flags, database, table); }
void Context::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return checkAccessImpl(flags, database, table, column); }
void Context::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return checkAccessImpl(flags, database, table, columns); }
void Context::checkAccess(const AccessFlags & flags, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return checkAccessImpl(flags, database, table, columns); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName()); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::string_view & column) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), column); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::vector<std::string_view> & columns) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), columns); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const Strings & columns) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), columns); }
void Context::checkAccess(const AccessRightsElement & element) const { return checkAccessImpl(element); }
void Context::checkAccess(const AccessRightsElements & elements) const { return checkAccessImpl(elements); }


std::shared_ptr<const ContextAccess> Context::getAccess() const
{
    auto lock = getLock();
    return access ? access : ContextAccess::getFullAccess();
}

ASTPtr Context::getRowPolicyCondition(const String & database, const String & table_name, RowPolicy::ConditionType type) const
{
    auto lock = getLock();
    auto initial_condition = initial_row_policy ? initial_row_policy->getCondition(database, table_name, type) : nullptr;
    return getAccess()->getRowPolicyCondition(database, table_name, type, initial_condition);
}

void Context::setInitialRowPolicy()
{
    auto lock = getLock();
    auto initial_user_id = getAccessControlManager().find<User>(client_info.initial_user);
    initial_row_policy = nullptr;
    if (initial_user_id)
        initial_row_policy = getAccessControlManager().getEnabledRowPolicies(*initial_user_id, {});
}


std::shared_ptr<const EnabledQuota> Context::getQuota() const
{
    return getAccess()->getQuota();
}


std::optional<QuotaUsage> Context::getQuotaUsage() const
{
    return getAccess()->getQuotaUsage();
}


void Context::setProfile(const String & profile_name)
{
    SettingsChanges profile_settings_changes = *getAccessControlManager().getProfileSettings(profile_name);
    try
    {
        checkSettingsConstraints(profile_settings_changes);
    }
    catch (Exception & e)
    {
        e.addMessage(", while trying to set settings profile {}", profile_name);
        throw;
    }
    applySettingsChanges(profile_settings_changes);
}


const Scalars & Context::getScalars() const
{
    return scalars;
}


const Block & Context::getScalar(const String & name) const
{
    auto it = scalars.find(name);
    if (scalars.end() == it)
    {
        // This should be a logical error, but it fails the sql_fuzz test too
        // often, so 'bad arguments' for now.
        throw Exception("Scalar " + backQuoteIfNeed(name) + " doesn't exist (internal bug)", ErrorCodes::BAD_ARGUMENTS);
    }
    return it->second;
}


Tables Context::getExternalTables() const
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    auto lock = getLock();

    Tables res;
    for (const auto & table : external_tables_mapping)
        res[table.first] = table.second->getTable();

    auto query_context_ptr = query_context.lock();
    auto session_context_ptr = session_context.lock();
    if (query_context_ptr && query_context_ptr.get() != this)
    {
        Tables buf = query_context_ptr->getExternalTables();
        res.insert(buf.begin(), buf.end());
    }
    else if (session_context_ptr && session_context_ptr.get() != this)
    {
        Tables buf = session_context_ptr->getExternalTables();
        res.insert(buf.begin(), buf.end());
    }
    return res;
}


void Context::addExternalTable(const String & table_name, TemporaryTableHolder && temporary_table)
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    auto lock = getLock();
    if (external_tables_mapping.end() != external_tables_mapping.find(table_name))
        throw Exception("Temporary table " + backQuoteIfNeed(table_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
    external_tables_mapping.emplace(table_name, std::make_shared<TemporaryTableHolder>(std::move(temporary_table)));
}


std::shared_ptr<TemporaryTableHolder> Context::removeExternalTable(const String & table_name)
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    std::shared_ptr<TemporaryTableHolder> holder;
    {
        auto lock = getLock();
        auto iter = external_tables_mapping.find(table_name);
        if (iter == external_tables_mapping.end())
            return {};
        holder = iter->second;
        external_tables_mapping.erase(iter);
    }
    return holder;
}


void Context::addScalar(const String & name, const Block & block)
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    scalars[name] = block;
}


bool Context::hasScalar(const String & name) const
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    return scalars.count(name);
}


void Context::addQueryAccessInfo(const String & quoted_database_name, const String & full_quoted_table_name, const Names & column_names)
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    std::lock_guard<std::mutex> lock(query_access_info.mutex);
    query_access_info.databases.emplace(quoted_database_name);
    query_access_info.tables.emplace(full_quoted_table_name);
    for (const auto & column_name : column_names)
        query_access_info.columns.emplace(full_quoted_table_name + "." + backQuoteIfNeed(column_name));
}


void Context::addQueryFactoriesInfo(QueryLogFactories factory_type, const String & created_object) const
{
    assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);
    auto lock = getLock();

    switch (factory_type)
    {
        case QueryLogFactories::AggregateFunction:
            query_factories_info.aggregate_functions.emplace(created_object);
            break;
        case QueryLogFactories::AggregateFunctionCombinator:
            query_factories_info.aggregate_function_combinators.emplace(created_object);
            break;
        case QueryLogFactories::Database:
            query_factories_info.database_engines.emplace(created_object);
            break;
        case QueryLogFactories::DataType:
            query_factories_info.data_type_families.emplace(created_object);
            break;
        case QueryLogFactories::Dictionary:
            query_factories_info.dictionaries.emplace(created_object);
            break;
        case QueryLogFactories::Format:
            query_factories_info.formats.emplace(created_object);
            break;
        case QueryLogFactories::Function:
            query_factories_info.functions.emplace(created_object);
            break;
        case QueryLogFactories::Storage:
            query_factories_info.storages.emplace(created_object);
            break;
        case QueryLogFactories::TableFunction:
            query_factories_info.table_functions.emplace(created_object);
    }
}


StoragePtr Context::executeTableFunction(const ASTPtr & table_expression)
{
    /// Slightly suboptimal.
    auto hash = table_expression->getTreeHash();
    String key = toString(hash.first) + '_' + toString(hash.second);

    StoragePtr & res = table_function_results[key];

    if (!res)
    {
        TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_expression, shared_from_this());

        /// Run it and remember the result
        res = table_function_ptr->execute(table_expression, shared_from_this(), table_function_ptr->getName());
    }

    return res;
}


void Context::addViewSource(const StoragePtr & storage)
{
    if (view_source)
        throw Exception(
            "Temporary view source storage " + backQuoteIfNeed(view_source->getName()) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
    view_source = storage;
}


StoragePtr Context::getViewSource()
{
    return view_source;
}

Settings Context::getSettings() const
{
    auto lock = getLock();
    return settings;
}


void Context::setSettings(const Settings & settings_)
{
    auto lock = getLock();
    auto old_readonly = settings.readonly;
    auto old_allow_ddl = settings.allow_ddl;
    auto old_allow_introspection_functions = settings.allow_introspection_functions;

    settings = settings_;

    if ((settings.readonly != old_readonly) || (settings.allow_ddl != old_allow_ddl) || (settings.allow_introspection_functions != old_allow_introspection_functions))
        calculateAccessRights();
}


void Context::setSetting(const StringRef & name, const String & value)
{
    auto lock = getLock();
    if (name == "profile")
    {
        setProfile(value);
        return;
    }
    settings.set(std::string_view{name}, value);

    if (name == "readonly" || name == "allow_ddl" || name == "allow_introspection_functions")
        calculateAccessRights();
}


void Context::setSetting(const StringRef & name, const Field & value)
{
    auto lock = getLock();
    if (name == "profile")
    {
        setProfile(value.safeGet<String>());
        return;
    }
    settings.set(std::string_view{name}, value);

    if (name == "readonly" || name == "allow_ddl" || name == "allow_introspection_functions")
        calculateAccessRights();
}


void Context::applySettingChange(const SettingChange & change)
{
    try
    {
        setSetting(change.name, change.value);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("in attempt to set the value of setting '{}' to {}",
                                 change.name, applyVisitor(FieldVisitorToString(), change.value)));
        throw;
    }
}


void Context::applySettingsChanges(const SettingsChanges & changes)
{
    auto lock = getLock();
    for (const SettingChange & change : changes)
        applySettingChange(change);
    applySettingsQuirks(settings);
}


void Context::checkSettingsConstraints(const SettingChange & change) const
{
    if (auto settings_constraints = getSettingsConstraints())
        settings_constraints->check(settings, change);
}

void Context::checkSettingsConstraints(const SettingsChanges & changes) const
{
    if (auto settings_constraints = getSettingsConstraints())
        settings_constraints->check(settings, changes);
}

void Context::checkSettingsConstraints(SettingsChanges & changes) const
{
    if (auto settings_constraints = getSettingsConstraints())
        settings_constraints->check(settings, changes);
}

void Context::clampToSettingsConstraints(SettingsChanges & changes) const
{
    if (auto settings_constraints = getSettingsConstraints())
        settings_constraints->clamp(settings, changes);
}

std::shared_ptr<const SettingsConstraints> Context::getSettingsConstraints() const
{
    return getAccess()->getSettingsConstraints();
}


String Context::getCurrentDatabase() const
{
    auto lock = getLock();
    return current_database;
}


String Context::getInitialQueryId() const
{
    return client_info.initial_query_id;
}


void Context::setCurrentDatabaseNameInGlobalContext(const String & name)
{
    if (!isGlobalContext())
        throw Exception("Cannot set current database for non global context, this method should be used during server initialization",
                        ErrorCodes::LOGICAL_ERROR);
    auto lock = getLock();

    if (!current_database.empty())
        throw Exception("Default database name cannot be changed in global context without server restart",
                        ErrorCodes::LOGICAL_ERROR);

    current_database = name;
}

void Context::setCurrentDatabase(const String & name)
{
    DatabaseCatalog::instance().assertDatabaseExists(name);
    auto lock = getLock();
    current_database = name;
    calculateAccessRights();
}

void Context::setCurrentQueryId(const String & query_id)
{
    /// Generate random UUID, but using lower quality RNG,
    ///  because Poco::UUIDGenerator::generateRandom method is using /dev/random, that is very expensive.
    /// NOTE: Actually we don't need to use UUIDs for query identifiers.
    /// We could use any suitable string instead.
    union
    {
        char bytes[16];
        struct
        {
            UInt64 a;
            UInt64 b;
        } words;
        __uint128_t uuid;
    } random;

    random.words.a = thread_local_rng(); //-V656
    random.words.b = thread_local_rng(); //-V656

    if (client_info.client_trace_context.trace_id != 0)
    {
        // Use the OpenTelemetry trace context we received from the client, and
        // create a new span for the query.
        query_trace_context = client_info.client_trace_context;
        query_trace_context.span_id = thread_local_rng();
    }
    else if (client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        // If this is an initial query without any parent OpenTelemetry trace, we
        // might start the trace ourselves, with some configurable probability.
        std::bernoulli_distribution should_start_trace{
            settings.opentelemetry_start_trace_probability};

        if (should_start_trace(thread_local_rng))
        {
            // Use the randomly generated default query id as the new trace id.
            query_trace_context.trace_id = random.uuid;
            query_trace_context.span_id = thread_local_rng();
            // Mark this trace as sampled in the flags.
            query_trace_context.trace_flags = 1;
        }
    }

    String query_id_to_set = query_id;
    if (query_id_to_set.empty())    /// If the user did not submit his query_id, then we generate it ourselves.
    {
        /// Use protected constructor.
        struct QueryUUID : Poco::UUID
        {
            QueryUUID(const char * bytes, Poco::UUID::Version version)
                : Poco::UUID(bytes, version) {}
        };

        query_id_to_set = QueryUUID(random.bytes, Poco::UUID::UUID_RANDOM).toString();
    }

    client_info.current_query_id = query_id_to_set;
}

void Context::killCurrentQuery()
{
    if (process_list_elem)
    {
        process_list_elem->cancelQuery(true);
    }
};

String Context::getDefaultFormat() const
{
    return default_format.empty() ? "TabSeparated" : default_format;
}


void Context::setDefaultFormat(const String & name)
{
    default_format = name;
}

MultiVersion<Macros>::Version Context::getMacros() const
{
    return shared->macros.get();
}

void Context::setMacros(std::unique_ptr<Macros> && macros)
{
    shared->macros.set(std::move(macros));
}

ContextPtr Context::getQueryContext() const
{
    auto ptr = query_context.lock();
    if (!ptr) throw Exception("There is no query or query context has expired", ErrorCodes::THERE_IS_NO_QUERY);
    return ptr;
}

bool Context::isInternalSubquery() const
{
    auto ptr = query_context.lock();
    return ptr && ptr.get() != this;
}

ContextPtr Context::getSessionContext() const
{
    auto ptr = session_context.lock();
    if (!ptr) throw Exception("There is no session or session context has expired", ErrorCodes::THERE_IS_NO_SESSION);
    return ptr;
}

ContextPtr Context::getGlobalContext() const
{
    auto ptr = global_context.lock();
    if (!ptr) throw Exception("There is no global context or global context has expired", ErrorCodes::LOGICAL_ERROR);
    return ptr;
}

ContextPtr Context::getBufferContext() const
{
    if (!buffer_context) throw Exception("There is no buffer context", ErrorCodes::LOGICAL_ERROR);
    return buffer_context;
}


const EmbeddedDictionaries & Context::getEmbeddedDictionaries() const
{
    return getEmbeddedDictionariesImpl(false);
}

EmbeddedDictionaries & Context::getEmbeddedDictionaries()
{
    return getEmbeddedDictionariesImpl(false);
}


const ExternalDictionariesLoader & Context::getExternalDictionariesLoader() const
{
    return const_cast<Context *>(this)->getExternalDictionariesLoader();
}

ExternalDictionariesLoader & Context::getExternalDictionariesLoader()
{
    std::lock_guard lock(shared->external_dictionaries_mutex);
    if (!shared->external_dictionaries_loader)
        shared->external_dictionaries_loader.emplace(getGlobalContext());
    return *shared->external_dictionaries_loader;
}


const ExternalModelsLoader & Context::getExternalModelsLoader() const
{
    return const_cast<Context *>(this)->getExternalModelsLoader();
}

ExternalModelsLoader & Context::getExternalModelsLoader()
{
    std::lock_guard lock(shared->external_models_mutex);
    return getExternalModelsLoaderUnlocked();
}

ExternalModelsLoader & Context::getExternalModelsLoaderUnlocked()
{
    if (!shared->external_models_loader)
        shared->external_models_loader.emplace(getGlobalContext());
    return *shared->external_models_loader;
}

void Context::setExternalModelsConfig(const ConfigurationPtr & config, const std::string & config_name)
{
    std::lock_guard lock(shared->external_models_mutex);

    if (shared->external_models_config && isSameConfigurationWithMultipleKeys(*config, *shared->external_models_config, "", config_name))
        return;

    shared->external_models_config = config;
    shared->models_repository_guard .reset();
    shared->models_repository_guard = getExternalModelsLoaderUnlocked().addConfigRepository(
        std::make_unique<ExternalLoaderXMLConfigRepository>(*config, config_name));
}


EmbeddedDictionaries & Context::getEmbeddedDictionariesImpl(const bool throw_on_error) const
{
    std::lock_guard lock(shared->embedded_dictionaries_mutex);

    if (!shared->embedded_dictionaries)
    {
        auto geo_dictionaries_loader = std::make_unique<GeoDictionariesLoader>();

        shared->embedded_dictionaries.emplace(
            std::move(geo_dictionaries_loader),
            getGlobalContext(),
            throw_on_error);
    }

    return *shared->embedded_dictionaries;
}


void Context::tryCreateEmbeddedDictionaries() const
{
    static_cast<void>(getEmbeddedDictionariesImpl(true));
}


void Context::setProgressCallback(ProgressCallback callback)
{
    /// Callback is set to a session or to a query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    progress_callback = callback;
}

ProgressCallback Context::getProgressCallback() const
{
    return progress_callback;
}


void Context::setProcessListElement(ProcessList::Element * elem)
{
    /// Set to a session or query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    process_list_elem = elem;
}

ProcessList::Element * Context::getProcessListElement() const
{
    return process_list_elem;
}


void Context::setUncompressedCache(size_t max_size_in_bytes)
{
    auto lock = getLock();

    if (shared->uncompressed_cache)
        throw Exception("Uncompressed cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->uncompressed_cache = std::make_shared<UncompressedCache>(max_size_in_bytes);
}


UncompressedCachePtr Context::getUncompressedCache() const
{
    auto lock = getLock();
    return shared->uncompressed_cache;
}


void Context::dropUncompressedCache() const
{
    auto lock = getLock();
    if (shared->uncompressed_cache)
        shared->uncompressed_cache->reset();
}


void Context::setMarkCache(size_t cache_size_in_bytes)
{
    auto lock = getLock();

    if (shared->mark_cache)
        throw Exception("Mark cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->mark_cache = std::make_shared<MarkCache>(cache_size_in_bytes);
}

MarkCachePtr Context::getMarkCache() const
{
    auto lock = getLock();
    return shared->mark_cache;
}

void Context::dropMarkCache() const
{
    auto lock = getLock();
    if (shared->mark_cache)
        shared->mark_cache->reset();
}


void Context::setMMappedFileCache(size_t cache_size_in_num_entries)
{
    auto lock = getLock();

    if (shared->mmap_cache)
        throw Exception("Mapped file cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->mmap_cache = std::make_shared<MMappedFileCache>(cache_size_in_num_entries);
}

MMappedFileCachePtr Context::getMMappedFileCache() const
{
    auto lock = getLock();
    return shared->mmap_cache;
}

void Context::dropMMappedFileCache() const
{
    auto lock = getLock();
    if (shared->mmap_cache)
        shared->mmap_cache->reset();
}


void Context::dropCaches() const
{
    auto lock = getLock();

    if (shared->uncompressed_cache)
        shared->uncompressed_cache->reset();

    if (shared->mark_cache)
        shared->mark_cache->reset();

    if (shared->mmap_cache)
        shared->mmap_cache->reset();
}

BackgroundSchedulePool & Context::getBufferFlushSchedulePool() const
{
    auto lock = getLock();
    if (!shared->buffer_flush_schedule_pool)
        shared->buffer_flush_schedule_pool.emplace(
            settings.background_buffer_flush_schedule_pool_size,
            CurrentMetrics::BackgroundBufferFlushSchedulePoolTask,
            "BgBufSchPool");
    return *shared->buffer_flush_schedule_pool;
}

BackgroundTaskSchedulingSettings Context::getBackgroundProcessingTaskSchedulingSettings() const
{
    BackgroundTaskSchedulingSettings task_settings;

    const auto & config = getConfigRef();
    task_settings.thread_sleep_seconds = config.getDouble("background_processing_pool_thread_sleep_seconds", 10);
    task_settings.thread_sleep_seconds_random_part = config.getDouble("background_processing_pool_thread_sleep_seconds_random_part", 1.0);
    task_settings.thread_sleep_seconds_if_nothing_to_do = config.getDouble("background_processing_pool_thread_sleep_seconds_if_nothing_to_do", 0.1);
    task_settings.task_sleep_seconds_when_no_work_min = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_min", 10);
    task_settings.task_sleep_seconds_when_no_work_max = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_max", 600);
    task_settings.task_sleep_seconds_when_no_work_multiplier = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_multiplier", 1.1);
    task_settings.task_sleep_seconds_when_no_work_random_part = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_random_part", 1.0);
    return task_settings;
}

BackgroundTaskSchedulingSettings Context::getBackgroundMoveTaskSchedulingSettings() const
{
    BackgroundTaskSchedulingSettings task_settings;

    const auto & config = getConfigRef();
    task_settings.thread_sleep_seconds = config.getDouble("background_move_processing_pool_thread_sleep_seconds", 10);
    task_settings.thread_sleep_seconds_random_part = config.getDouble("background_move_processing_pool_thread_sleep_seconds_random_part", 1.0);
    task_settings.thread_sleep_seconds_if_nothing_to_do = config.getDouble("background_move_processing_pool_thread_sleep_seconds_if_nothing_to_do", 0.1);
    task_settings.task_sleep_seconds_when_no_work_min = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_min", 10);
    task_settings.task_sleep_seconds_when_no_work_max = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_max", 600);
    task_settings.task_sleep_seconds_when_no_work_multiplier = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_multiplier", 1.1);
    task_settings.task_sleep_seconds_when_no_work_random_part = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_random_part", 1.0);

    return task_settings;
}

BackgroundSchedulePool & Context::getSchedulePool() const
{
    auto lock = getLock();
    if (!shared->schedule_pool)
        shared->schedule_pool.emplace(
            settings.background_schedule_pool_size,
            CurrentMetrics::BackgroundSchedulePoolTask,
            "BgSchPool");
    return *shared->schedule_pool;
}

BackgroundSchedulePool & Context::getDistributedSchedulePool() const
{
    auto lock = getLock();
    if (!shared->distributed_schedule_pool)
        shared->distributed_schedule_pool.emplace(
            settings.background_distributed_schedule_pool_size,
            CurrentMetrics::BackgroundDistributedSchedulePoolTask,
            "BgDistSchPool");
    return *shared->distributed_schedule_pool;
}

BackgroundSchedulePool & Context::getMessageBrokerSchedulePool() const
{
    auto lock = getLock();
    if (!shared->message_broker_schedule_pool)
        shared->message_broker_schedule_pool.emplace(
            settings.background_message_broker_schedule_pool_size,
            CurrentMetrics::BackgroundDistributedSchedulePoolTask,
            "BgMBSchPool");
    return *shared->message_broker_schedule_pool;
}

bool Context::hasDistributedDDL() const
{
    return getConfigRef().has("distributed_ddl");
}

void Context::setDDLWorker(std::unique_ptr<DDLWorker> ddl_worker)
{
    auto lock = getLock();
    if (shared->ddl_worker)
        throw Exception("DDL background thread has already been initialized", ErrorCodes::LOGICAL_ERROR);
    ddl_worker->startup();
    shared->ddl_worker = std::move(ddl_worker);
}

DDLWorker & Context::getDDLWorker() const
{
    auto lock = getLock();
    if (!shared->ddl_worker)
    {
        if (!hasZooKeeper())
            throw Exception("There is no Zookeeper configuration in server config", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

        if (!hasDistributedDDL())
            throw Exception("There is no DistributedDDL configuration in server config", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

        throw Exception("DDL background thread is not initialized", ErrorCodes::NO_ELEMENTS_IN_CONFIG);
    }
    return *shared->ddl_worker;
}

zkutil::ZooKeeperPtr Context::getZooKeeper() const
{
    std::lock_guard lock(shared->zookeeper_mutex);

    const auto & config = shared->zookeeper_config ? *shared->zookeeper_config : getConfigRef();
    if (!shared->zookeeper)
        shared->zookeeper = std::make_shared<zkutil::ZooKeeper>(config, "zookeeper");
    else if (shared->zookeeper->expired())
        shared->zookeeper = shared->zookeeper->startNewSession();

    return shared->zookeeper;
}


void Context::initializeKeeperStorageDispatcher() const
{
#if USE_NURAFT
    std::lock_guard lock(shared->nu_keeper_storage_dispatcher_mutex);

    if (shared->nu_keeper_storage_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to initialize Keeper multiple times");

    const auto & config = getConfigRef();
    if (config.has("keeper_server"))
    {
        shared->nu_keeper_storage_dispatcher = std::make_shared<KeeperStorageDispatcher>();
        shared->nu_keeper_storage_dispatcher->initialize(config);
    }
#endif
}

#if USE_NURAFT
std::shared_ptr<KeeperStorageDispatcher> & Context::getKeeperStorageDispatcher() const
{
    std::lock_guard lock(shared->nu_keeper_storage_dispatcher_mutex);
    if (!shared->nu_keeper_storage_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Keeper must be initialized before requests");

    return shared->nu_keeper_storage_dispatcher;
}
#endif

void Context::shutdownKeeperStorageDispatcher() const
{
#if USE_NURAFT
    std::lock_guard lock(shared->nu_keeper_storage_dispatcher_mutex);
    if (shared->nu_keeper_storage_dispatcher)
    {
        shared->nu_keeper_storage_dispatcher->shutdown();
        shared->nu_keeper_storage_dispatcher.reset();
    }
#endif
}


zkutil::ZooKeeperPtr Context::getAuxiliaryZooKeeper(const String & name) const
{
    std::lock_guard lock(shared->auxiliary_zookeepers_mutex);

    auto zookeeper = shared->auxiliary_zookeepers.find(name);
    if (zookeeper == shared->auxiliary_zookeepers.end())
    {
        const auto & config = shared->auxiliary_zookeepers_config ? *shared->auxiliary_zookeepers_config : getConfigRef();
        if (!config.has("auxiliary_zookeepers." + name))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown auxiliary ZooKeeper name '{}'. If it's required it can be added to the section <auxiliary_zookeepers> in "
                "config.xml",
                name);

        zookeeper
            = shared->auxiliary_zookeepers.emplace(name, std::make_shared<zkutil::ZooKeeper>(config, "auxiliary_zookeepers." + name)).first;
    }
    else if (zookeeper->second->expired())
        zookeeper->second = zookeeper->second->startNewSession();

    return zookeeper->second;
}

void Context::resetZooKeeper() const
{
    std::lock_guard lock(shared->zookeeper_mutex);
    shared->zookeeper.reset();
}

static void reloadZooKeeperIfChangedImpl(const ConfigurationPtr & config, const std::string & config_name, zkutil::ZooKeeperPtr & zk)
{
    if (!zk || zk->configChanged(*config, config_name))
    {
        if (zk)
            zk->finalize();

        zk = std::make_shared<zkutil::ZooKeeper>(*config, config_name);
    }
}

void Context::reloadZooKeeperIfChanged(const ConfigurationPtr & config) const
{
    std::lock_guard lock(shared->zookeeper_mutex);
    shared->zookeeper_config = config;
    reloadZooKeeperIfChangedImpl(config, "zookeeper", shared->zookeeper);
}

void Context::reloadAuxiliaryZooKeepersConfigIfChanged(const ConfigurationPtr & config)
{
    std::lock_guard lock(shared->auxiliary_zookeepers_mutex);

    shared->auxiliary_zookeepers_config = config;

    for (auto it = shared->auxiliary_zookeepers.begin(); it != shared->auxiliary_zookeepers.end();)
    {
        if (!config->has("auxiliary_zookeepers." + it->first))
            it = shared->auxiliary_zookeepers.erase(it);
        else
        {
            reloadZooKeeperIfChangedImpl(config, "auxiliary_zookeepers." + it->first, it->second);
            ++it;
        }
    }
}


bool Context::hasZooKeeper() const
{
    return getConfigRef().has("zookeeper");
}

bool Context::hasAuxiliaryZooKeeper(const String & name) const
{
    return getConfigRef().has("auxiliary_zookeepers." + name);
}

InterserverCredentialsPtr Context::getInterserverCredentials()
{
    return shared->interserver_io_credentials.get();
}

void Context::updateInterserverCredentials(const Poco::Util::AbstractConfiguration & config)
{
    auto credentials = InterserverCredentials::make(config, "interserver_http_credentials");
    shared->interserver_io_credentials.set(std::move(credentials));
}

void Context::setInterserverIOAddress(const String & host, UInt16 port)
{
    shared->interserver_io_host = host;
    shared->interserver_io_port = port;
}

std::pair<String, UInt16> Context::getInterserverIOAddress() const
{
    if (shared->interserver_io_host.empty() || shared->interserver_io_port == 0)
        throw Exception("Parameter 'interserver_http(s)_port' required for replication is not specified in configuration file.",
                        ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    return { shared->interserver_io_host, shared->interserver_io_port };
}

void Context::setInterserverScheme(const String & scheme)
{
    shared->interserver_scheme = scheme;
}

String Context::getInterserverScheme() const
{
    return shared->interserver_scheme;
}

void Context::setRemoteHostFilter(const Poco::Util::AbstractConfiguration & config)
{
    shared->remote_host_filter.setValuesFromConfig(config);
}

const RemoteHostFilter & Context::getRemoteHostFilter() const
{
    return shared->remote_host_filter;
}

UInt16 Context::getTCPPort() const
{
    auto lock = getLock();

    const auto & config = getConfigRef();
    return config.getInt("tcp_port", DBMS_DEFAULT_PORT);
}

std::optional<UInt16> Context::getTCPPortSecure() const
{
    auto lock = getLock();

    const auto & config = getConfigRef();
    if (config.has("tcp_port_secure"))
        return config.getInt("tcp_port_secure");
    return {};
}

std::shared_ptr<Cluster> Context::getCluster(const std::string & cluster_name) const
{
    auto res = getClusters().getCluster(cluster_name);
    if (res)
        return res;

    res = tryGetReplicatedDatabaseCluster(cluster_name);
    if (res)
        return res;

    throw Exception("Requested cluster '" + cluster_name + "' not found", ErrorCodes::BAD_GET);
}


std::shared_ptr<Cluster> Context::tryGetCluster(const std::string & cluster_name) const
{
    return getClusters().getCluster(cluster_name);
}


void Context::reloadClusterConfig()
{
    while (true)
    {
        ConfigurationPtr cluster_config;
        {
            std::lock_guard lock(shared->clusters_mutex);
            cluster_config = shared->clusters_config;
        }

        const auto & config = cluster_config ? *cluster_config : getConfigRef();
        auto new_clusters = std::make_unique<Clusters>(config, settings);

        {
            std::lock_guard lock(shared->clusters_mutex);
            if (shared->clusters_config.get() == cluster_config.get())
            {
                shared->clusters = std::move(new_clusters);
                return;
            }

            // Clusters config has been suddenly changed, recompute clusters
        }
    }
}


Clusters & Context::getClusters() const
{
    std::lock_guard lock(shared->clusters_mutex);
    if (!shared->clusters)
    {
        const auto & config = shared->clusters_config ? *shared->clusters_config : getConfigRef();
        shared->clusters = std::make_unique<Clusters>(config, settings);
    }

    return *shared->clusters;
}


/// On repeating calls updates existing clusters and adds new clusters, doesn't delete old clusters
void Context::setClustersConfig(const ConfigurationPtr & config, const String & config_name)
{
    std::lock_guard lock(shared->clusters_mutex);

    /// Do not update clusters if this part of config wasn't changed.
    if (shared->clusters && isSameConfiguration(*config, *shared->clusters_config, config_name))
        return;

    auto old_clusters_config = shared->clusters_config;
    shared->clusters_config = config;

    if (!shared->clusters)
        shared->clusters = std::make_unique<Clusters>(*shared->clusters_config, settings, config_name);
    else
        shared->clusters->updateClusters(*shared->clusters_config, settings, config_name, old_clusters_config);
}


void Context::setCluster(const String & cluster_name, const std::shared_ptr<Cluster> & cluster)
{
    std::lock_guard lock(shared->clusters_mutex);

    if (!shared->clusters)
        throw Exception("Clusters are not set", ErrorCodes::LOGICAL_ERROR);

    shared->clusters->setCluster(cluster_name, cluster);
}


void Context::initializeSystemLogs()
{
    auto lock = getLock();
    shared->system_logs = std::make_unique<SystemLogs>(getGlobalContext(), getConfigRef());
}

void Context::initializeTraceCollector()
{
    shared->initializeTraceCollector(getTraceLog());
}

bool Context::hasTraceCollector() const
{
    return shared->hasTraceCollector();
}


std::shared_ptr<QueryLog> Context::getQueryLog()
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_log;
}


std::shared_ptr<QueryThreadLog> Context::getQueryThreadLog()
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_thread_log;
}


std::shared_ptr<PartLog> Context::getPartLog(const String & part_database)
{
    auto lock = getLock();

    /// No part log or system logs are shutting down.
    if (!shared->system_logs)
        return {};

    /// Will not log operations on system tables (including part_log itself).
    /// It doesn't make sense and not allow to destruct PartLog correctly due to infinite logging and flushing,
    /// and also make troubles on startup.
    if (part_database == DatabaseCatalog::SYSTEM_DATABASE)
        return {};

    return shared->system_logs->part_log;
}


std::shared_ptr<TraceLog> Context::getTraceLog()
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->trace_log;
}


std::shared_ptr<TextLog> Context::getTextLog()
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->text_log;
}


std::shared_ptr<MetricLog> Context::getMetricLog()
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->metric_log;
}


std::shared_ptr<AsynchronousMetricLog> Context::getAsynchronousMetricLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->asynchronous_metric_log;
}


std::shared_ptr<OpenTelemetrySpanLog> Context::getOpenTelemetrySpanLog()
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->opentelemetry_span_log;
}


CompressionCodecPtr Context::chooseCompressionCodec(size_t part_size, double part_size_ratio) const
{
    auto lock = getLock();

    if (!shared->compression_codec_selector)
    {
        constexpr auto config_name = "compression";
        const auto & config = getConfigRef();

        if (config.has(config_name))
            shared->compression_codec_selector = std::make_unique<CompressionCodecSelector>(config, "compression");
        else
            shared->compression_codec_selector = std::make_unique<CompressionCodecSelector>();
    }

    return shared->compression_codec_selector->choose(part_size, part_size_ratio);
}


DiskPtr Context::getDisk(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto disk_selector = getDiskSelector(lock);

    return disk_selector->get(name);
}

StoragePolicyPtr Context::getStoragePolicy(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto policy_selector = getStoragePolicySelector(lock);

    return policy_selector->get(name);
}


DisksMap Context::getDisksMap() const
{
    std::lock_guard lock(shared->storage_policies_mutex);
    return getDiskSelector(lock)->getDisksMap();
}

StoragePoliciesMap Context::getPoliciesMap() const
{
    std::lock_guard lock(shared->storage_policies_mutex);
    return getStoragePolicySelector(lock)->getPoliciesMap();
}

DiskSelectorPtr Context::getDiskSelector(std::lock_guard<std::mutex> & /* lock */) const
{
    if (!shared->merge_tree_disk_selector)
    {
        constexpr auto config_name = "storage_configuration.disks";
        const auto & config = getConfigRef();

        shared->merge_tree_disk_selector = std::make_shared<DiskSelector>(config, config_name, shared_from_this());
    }
    return shared->merge_tree_disk_selector;
}

StoragePolicySelectorPtr Context::getStoragePolicySelector(std::lock_guard<std::mutex> & lock) const
{
    if (!shared->merge_tree_storage_policy_selector)
    {
        constexpr auto config_name = "storage_configuration.policies";
        const auto & config = getConfigRef();

        shared->merge_tree_storage_policy_selector = std::make_shared<StoragePolicySelector>(config, config_name, getDiskSelector(lock));
    }
    return shared->merge_tree_storage_policy_selector;
}


void Context::updateStorageConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->storage_policies_mutex);

    if (shared->merge_tree_disk_selector)
        shared->merge_tree_disk_selector
            = shared->merge_tree_disk_selector->updateFromConfig(config, "storage_configuration.disks", shared_from_this());

    if (shared->merge_tree_storage_policy_selector)
    {
        try
        {
            shared->merge_tree_storage_policy_selector = shared->merge_tree_storage_policy_selector->updateFromConfig(
                config, "storage_configuration.policies", shared->merge_tree_disk_selector);
        }
        catch (Exception & e)
        {
            LOG_ERROR(
                shared->log, "An error has occurred while reloading storage policies, storage policies were not applied: {}", e.message());
        }
    }

#if !defined(ARCADIA_BUILD)
    if (shared->storage_s3_settings)
    {
        shared->storage_s3_settings->loadFromConfig("s3", config);
    }
#endif
}


const MergeTreeSettings & Context::getMergeTreeSettings() const
{
    auto lock = getLock();

    if (!shared->merge_tree_settings)
    {
        const auto & config = getConfigRef();
        MergeTreeSettings mt_settings;
        mt_settings.loadFromConfig("merge_tree", config);
        shared->merge_tree_settings.emplace(mt_settings);
    }

    return *shared->merge_tree_settings;
}

const MergeTreeSettings & Context::getReplicatedMergeTreeSettings() const
{
    auto lock = getLock();

    if (!shared->replicated_merge_tree_settings)
    {
        const auto & config = getConfigRef();
        MergeTreeSettings mt_settings;
        mt_settings.loadFromConfig("merge_tree", config);
        mt_settings.loadFromConfig("replicated_merge_tree", config);
        shared->replicated_merge_tree_settings.emplace(mt_settings);
    }

    return *shared->replicated_merge_tree_settings;
}

const StorageS3Settings & Context::getStorageS3Settings() const
{
#if !defined(ARCADIA_BUILD)
    auto lock = getLock();

    if (!shared->storage_s3_settings)
    {
        const auto & config = getConfigRef();
        shared->storage_s3_settings.emplace().loadFromConfig("s3", config);
    }

    return *shared->storage_s3_settings;
#else
    throw Exception("S3 is unavailable in Arcadia", ErrorCodes::NOT_IMPLEMENTED);
#endif
}

void Context::checkCanBeDropped(const String & database, const String & table, const size_t & size, const size_t & max_size_to_drop) const
{
    if (!max_size_to_drop || size <= max_size_to_drop)
        return;

    Poco::File force_file(getFlagsPath() + "force_drop_table");
    bool force_file_exists = force_file.exists();

    if (force_file_exists)
    {
        try
        {
            force_file.remove();
            return;
        }
        catch (...)
        {
            /// User should recreate force file on each drop, it shouldn't be protected
            tryLogCurrentException("Drop table check", "Can't remove force file to enable table or partition drop");
        }
    }

    String size_str = formatReadableSizeWithDecimalSuffix(size);
    String max_size_to_drop_str = formatReadableSizeWithDecimalSuffix(max_size_to_drop);
    throw Exception(ErrorCodes::TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT,
                    "Table or Partition in {}.{} was not dropped.\nReason:\n"
                    "1. Size ({}) is greater than max_[table/partition]_size_to_drop ({})\n"
                    "2. File '{}' intended to force DROP {}\n"
                    "How to fix this:\n"
                    "1. Either increase (or set to zero) max_[table/partition]_size_to_drop in server config\n"
                    "2. Either create forcing file {} and make sure that ClickHouse has write permission for it.\n"
                    "Example:\nsudo touch '{}' && sudo chmod 666 '{}'",
                    backQuoteIfNeed(database), backQuoteIfNeed(table),
                    size_str, max_size_to_drop_str,
                    force_file.path(), force_file_exists ? "exists but not writeable (could not be removed)" : "doesn't exist",
                    force_file.path(),
                    force_file.path(), force_file.path());
}


void Context::setMaxTableSizeToDrop(size_t max_size)
{
    // Is initialized at server startup and updated at config reload
    shared->max_table_size_to_drop.store(max_size, std::memory_order_relaxed);
}


void Context::checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size) const
{
    size_t max_table_size_to_drop = shared->max_table_size_to_drop.load(std::memory_order_relaxed);

    checkCanBeDropped(database, table, table_size, max_table_size_to_drop);
}


void Context::setMaxPartitionSizeToDrop(size_t max_size)
{
    // Is initialized at server startup and updated at config reload
    shared->max_partition_size_to_drop.store(max_size, std::memory_order_relaxed);
}


void Context::checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size) const
{
    size_t max_partition_size_to_drop = shared->max_partition_size_to_drop.load(std::memory_order_relaxed);

    checkCanBeDropped(database, table, partition_size, max_partition_size_to_drop);
}


BlockInputStreamPtr Context::getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, UInt64 max_block_size) const
{
    return std::make_shared<InputStreamFromInputFormat>(
        FormatFactory::instance().getInput(name, buf, sample, shared_from_this(), max_block_size));
}

BlockOutputStreamPtr Context::getOutputStreamParallelIfPossible(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutputStreamParallelIfPossible(name, buf, sample, shared_from_this());
}

BlockOutputStreamPtr Context::getOutputStream(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutputStream(name, buf, sample, shared_from_this());
}

OutputFormatPtr Context::getOutputFormatParallelIfPossible(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutputFormatParallelIfPossible(name, buf, sample, shared_from_this());
}

OutputFormatPtr Context::getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutputFormat(name, buf, sample, shared_from_this());
}


time_t Context::getUptimeSeconds() const
{
    auto lock = getLock();
    return shared->uptime_watch.elapsedSeconds();
}


void Context::setConfigReloadCallback(ConfigReloadCallback && callback)
{
    /// Is initialized at server startup, so lock isn't required. Otherwise use mutex.
    shared->config_reload_callback = std::move(callback);
}

void Context::reloadConfig() const
{
    /// Use mutex if callback may be changed after startup.
    if (!shared->config_reload_callback)
        throw Exception("Can't reload config because config_reload_callback is not set.", ErrorCodes::LOGICAL_ERROR);

    shared->config_reload_callback();
}


void Context::shutdown()
{
    // Disk selector might not be initialized if there was some error during
    // its initialization. Don't try to initialize it again on shutdown.
    if (shared->merge_tree_disk_selector)
    {
        for (auto & [disk_name, disk] : getDisksMap())
        {
            LOG_INFO(shared->log, "Shutdown disk {}", disk_name);
            disk->shutdown();
        }
    }

    shared->shutdown();
}


Context::ApplicationType Context::getApplicationType() const
{
    return shared->application_type;
}

void Context::setApplicationType(ApplicationType type)
{
    /// Lock isn't required, you should set it at start
    shared->application_type = type;
}

void Context::setDefaultProfiles(const Poco::Util::AbstractConfiguration & config)
{
    shared->default_profile_name = config.getString("default_profile", "default");
    getAccessControlManager().setDefaultProfileName(shared->default_profile_name);

    shared->system_profile_name = config.getString("system_profile", shared->default_profile_name);
    setProfile(shared->system_profile_name);

    applySettingsQuirks(settings, &Poco::Logger::get("SettingsQuirks"));

    shared->buffer_profile_name = config.getString("buffer_profile", shared->system_profile_name);
    buffer_context = Context::createCopy(shared_from_this());
    buffer_context->setProfile(shared->buffer_profile_name);
}

String Context::getDefaultProfileName() const
{
    return shared->default_profile_name;
}

String Context::getSystemProfileName() const
{
    return shared->system_profile_name;
}

String Context::getFormatSchemaPath() const
{
    return shared->format_schema_path;
}

void Context::setFormatSchemaPath(const String & path)
{
    shared->format_schema_path = path;
}

Context::SampleBlockCache & Context::getSampleBlockCache() const
{
    return getQueryContext()->sample_block_cache;
}


bool Context::hasQueryParameters() const
{
    return !query_parameters.empty();
}


const NameToNameMap & Context::getQueryParameters() const
{
    return query_parameters;
}


void Context::setQueryParameter(const String & name, const String & value)
{
    if (!query_parameters.emplace(name, value).second)
        throw Exception("Duplicate name " + backQuote(name) + " of query parameter", ErrorCodes::BAD_ARGUMENTS);
}


void Context::addBridgeCommand(std::unique_ptr<ShellCommand> cmd) const
{
    auto lock = getLock();
    shared->bridge_commands.emplace_back(std::move(cmd));
}


IHostContextPtr & Context::getHostContext()
{
    return host_context;
}


const IHostContextPtr & Context::getHostContext() const
{
    return host_context;
}


std::shared_ptr<ActionLocksManager> Context::getActionLocksManager()
{
    auto lock = getLock();

    if (!shared->action_locks_manager)
        shared->action_locks_manager = std::make_shared<ActionLocksManager>(shared_from_this());

    return shared->action_locks_manager;
}


void Context::setExternalTablesInitializer(ExternalTablesInitializer && initializer)
{
    if (external_tables_initializer_callback)
        throw Exception("External tables initializer is already set", ErrorCodes::LOGICAL_ERROR);

    external_tables_initializer_callback = std::move(initializer);
}

void Context::initializeExternalTablesIfSet()
{
    if (external_tables_initializer_callback)
    {
        external_tables_initializer_callback(shared_from_this());
        /// Reset callback
        external_tables_initializer_callback = {};
    }
}


void Context::setInputInitializer(InputInitializer && initializer)
{
    if (input_initializer_callback)
        throw Exception("Input initializer is already set", ErrorCodes::LOGICAL_ERROR);

    input_initializer_callback = std::move(initializer);
}


void Context::initializeInput(const StoragePtr & input_storage)
{
    if (!input_initializer_callback)
        throw Exception("Input initializer is not set", ErrorCodes::LOGICAL_ERROR);

    input_initializer_callback(shared_from_this(), input_storage);
    /// Reset callback
    input_initializer_callback = {};
}


void Context::setInputBlocksReaderCallback(InputBlocksReader && reader)
{
    if (input_blocks_reader)
        throw Exception("Input blocks reader is already set", ErrorCodes::LOGICAL_ERROR);

    input_blocks_reader = std::move(reader);
}


InputBlocksReader Context::getInputBlocksReaderCallback() const
{
    return input_blocks_reader;
}


void Context::resetInputCallbacks()
{
    if (input_initializer_callback)
        input_initializer_callback = {};

    if (input_blocks_reader)
        input_blocks_reader = {};
}


StorageID Context::resolveStorageID(StorageID storage_id, StorageNamespace where) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    StorageID resolved = StorageID::createEmpty();
    std::optional<Exception> exc;
    {
        auto lock = getLock();
        resolved = resolveStorageIDImpl(std::move(storage_id), where, &exc);
    }
    if (exc)
        throw Exception(*exc);
    if (!resolved.hasUUID() && resolved.database_name != DatabaseCatalog::TEMPORARY_DATABASE)
        resolved.uuid = DatabaseCatalog::instance().getDatabase(resolved.database_name)->tryGetTableUUID(resolved.table_name);
    return resolved;
}

StorageID Context::tryResolveStorageID(StorageID storage_id, StorageNamespace where) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    StorageID resolved = StorageID::createEmpty();
    {
        auto lock = getLock();
        resolved = resolveStorageIDImpl(std::move(storage_id), where, nullptr);
    }
    if (resolved && !resolved.hasUUID() && resolved.database_name != DatabaseCatalog::TEMPORARY_DATABASE)
    {
        auto db = DatabaseCatalog::instance().tryGetDatabase(resolved.database_name);
        if (db)
            resolved.uuid = db->tryGetTableUUID(resolved.table_name);
    }
    return resolved;
}

StorageID Context::resolveStorageIDImpl(StorageID storage_id, StorageNamespace where, std::optional<Exception> * exception) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    if (!storage_id)
    {
        if (exception)
            exception->emplace("Both table name and UUID are empty", ErrorCodes::UNKNOWN_TABLE);
        return storage_id;
    }

    bool look_for_external_table = where & StorageNamespace::ResolveExternal;
    bool in_current_database = where & StorageNamespace::ResolveCurrentDatabase;
    bool in_specified_database = where & StorageNamespace::ResolveGlobal;

    if (!storage_id.database_name.empty())
    {
        if (in_specified_database)
            return storage_id;     /// NOTE There is no guarantees that table actually exists in database.
        if (exception)
            exception->emplace("External and temporary tables have no database, but " +
                        storage_id.database_name + " is specified", ErrorCodes::UNKNOWN_TABLE);
        return StorageID::createEmpty();
    }

    /// Database name is not specified. It's temporary table or table in current database.

    if (look_for_external_table)
    {
        /// Global context should not contain temporary tables
        assert(!isGlobalContext() || getApplicationType() == ApplicationType::LOCAL);

        auto resolved_id = StorageID::createEmpty();
        auto try_resolve = [&](ContextConstPtr context) -> bool
        {
            const auto & tables = context->external_tables_mapping;
            auto it = tables.find(storage_id.getTableName());
            if (it == tables.end())
                return false;
            resolved_id = it->second->getGlobalTableID();
            return true;
        };

        /// Firstly look for temporary table in current context
        if (try_resolve(shared_from_this()))
            return resolved_id;

        /// If not found and current context was created from some query context, look for temporary table in query context
        auto query_context_ptr = query_context.lock();
        bool is_local_context = query_context_ptr && query_context_ptr.get() != this;
        if (is_local_context && try_resolve(query_context_ptr))
            return resolved_id;

        /// If not found and current context was created from some session context, look for temporary table in session context
        auto session_context_ptr = session_context.lock();
        bool is_local_or_query_context = session_context_ptr && session_context_ptr.get() != this;
        if (is_local_or_query_context && try_resolve(session_context_ptr))
            return resolved_id;
    }

    /// Temporary table not found. It's table in current database.

    if (in_current_database)
    {
        if (current_database.empty())
        {
            if (exception)
                exception->emplace("Default database is not selected", ErrorCodes::UNKNOWN_DATABASE);
            return StorageID::createEmpty();
        }
        storage_id.database_name = current_database;
        /// NOTE There is no guarantees that table actually exists in database.
        return storage_id;
    }

    if (exception)
        exception->emplace("Cannot resolve database name for table " + storage_id.getNameForLogs(), ErrorCodes::UNKNOWN_TABLE);
    return StorageID::createEmpty();
}

void Context::initZooKeeperMetadataTransaction(ZooKeeperMetadataTransactionPtr txn, [[maybe_unused]] bool attach_existing)
{
    assert(!metadata_transaction);
    assert(attach_existing || query_context.lock().get() == this);
    metadata_transaction = std::move(txn);
}

ZooKeeperMetadataTransactionPtr Context::getZooKeeperMetadataTransaction() const
{
    assert(!metadata_transaction || hasQueryContext());
    return metadata_transaction;
}

PartUUIDsPtr Context::getPartUUIDs()
{
    auto lock = getLock();
    if (!part_uuids)
        part_uuids = std::make_shared<PartUUIDs>();

    return part_uuids;
}


ReadTaskCallback Context::getReadTaskCallback() const
{
    if (!next_task_callback.has_value())
        throw Exception(fmt::format("Next task callback is not set for query {}", getInitialQueryId()), ErrorCodes::LOGICAL_ERROR);
    return next_task_callback.value();
}


void Context::setReadTaskCallback(ReadTaskCallback && callback)
{
    next_task_callback = callback;
}

PartUUIDsPtr Context::getIgnoredPartUUIDs()
{
    auto lock = getLock();
    if (!ignored_part_uuids)
        ignored_part_uuids = std::make_shared<PartUUIDs>();

    return ignored_part_uuids;
}

}
