#include <Interpreters/Session.h>

#include <base/isSharedPtrUnique.h>
#include <Access/AccessControl.h>
#include <Access/Credentials.h>
#include <Access/ContextAccess.h>
#include <Access/SettingsProfilesInfo.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <Interpreters/SessionTracker.h>
#include <Interpreters/Context.h>
#include <Interpreters/SessionLog.h>
#include <Interpreters/Cluster.h>

#include <base/EnumReflection.h>

#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <fmt/ranges.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_sessions_for_user;
    extern const SettingsBool push_external_roles_in_interserver_queries;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SESSION_NOT_FOUND;
    extern const int SESSION_IS_LOCKED;
    extern const int USER_EXPIRED;
}


class NamedSessionsStorage;

/// User ID and session identifier. Named sessions are local to users.
using NamedSessionKey = std::pair<UUID, String>;

/// Named sessions. The user could specify session identifier to reuse settings and temporary tables in subsequent requests.
struct NamedSessionData
{
    NamedSessionKey key;
    ContextMutablePtr context;
    std::chrono::steady_clock::duration timeout;
    std::chrono::steady_clock::time_point close_time_bucket{};
    NamedSessionsStorage & parent;

    NamedSessionData(NamedSessionKey key_, ContextPtr context_, std::chrono::steady_clock::duration timeout_, NamedSessionsStorage & parent_)
        : key(std::move(key_)), context(Context::createCopy(context_)), timeout(timeout_), parent(parent_)
    {}

    void release();
};

class NamedSessionsStorage
{
public:
    using Key = NamedSessionKey;

    static NamedSessionsStorage & instance()
    {
        static NamedSessionsStorage the_instance;
        return the_instance;
    }

    ~NamedSessionsStorage()
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

    void shutdown()
    {
        {
            std::lock_guard lock{mutex};
            sessions.clear();
            if (!thread.joinable())
                return;
            quit = true;
        }

        cond.notify_one();
        thread.join();
    }

    /// Find existing session or create a new.
    std::pair<std::shared_ptr<NamedSessionData>, bool> acquireSession(
        const ContextPtr & global_context,
        const UUID & user_id,
        const String & session_id,
        std::chrono::steady_clock::duration timeout,
        bool throw_if_not_found)
    {
        std::unique_lock lock(mutex);

        Key key{user_id, session_id};

        auto it = sessions.find(key);
        if (it == sessions.end())
        {
            if (throw_if_not_found)
                throw Exception(ErrorCodes::SESSION_NOT_FOUND, "Session {} not found", session_id);

            /// Create a new session from current context.
            it = sessions.insert(std::make_pair(key, std::make_shared<NamedSessionData>(key, global_context, timeout, *this))).first;
            const auto & session = it->second;

            if (!thread.joinable())
                thread = ThreadFromGlobalPool{&NamedSessionsStorage::cleanThread, this};

            LOG_TRACE(log, "Create new session with session_id: {}, user_id: {}", key.second, toString(key.first));

            return {session, true};
        }

        /// Use existing session.
        const auto & session = it->second;

        LOG_TRACE(log, "Reuse session from storage with session_id: {}, user_id: {}", key.second, toString(key.first));

        if (!isSharedPtrUnique(session))
            throw Exception(ErrorCodes::SESSION_IS_LOCKED, "Session {} is locked by a concurrent client", session_id);

        if (session->close_time_bucket != std::chrono::steady_clock::time_point{})
        {
            auto bucket_it = close_time_buckets.find(session->close_time_bucket);
            auto & bucket_sessions = bucket_it->second;
            bucket_sessions.erase(key);
            if (bucket_sessions.empty())
                close_time_buckets.erase(bucket_it);

            session->close_time_bucket = std::chrono::steady_clock::time_point{};
        }

        return {session, false};
    }

    void releaseSession(NamedSessionData & session)
    {
        std::unique_lock lock(mutex);
        scheduleCloseSession(session, lock);
    }

    void releaseAndCloseSession(const UUID & user_id, const String & session_id, std::shared_ptr<NamedSessionData> & session_data)
    {
        std::unique_lock lock(mutex);
        session_data = nullptr;

        Key key{user_id, session_id};
        auto it = sessions.find(key);
        if (it == sessions.end())
        {
            LOG_INFO(log, "Session {} not found for user {}, probably it's already closed", session_id, toString(user_id));
            return;
        }

        if (!isSharedPtrUnique(it->second))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot close session {} with refcount {}", session_id, it->second.use_count());

        sessions.erase(it);
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

    using Container = std::unordered_map<Key, std::shared_ptr<NamedSessionData>, SessionKeyHash>;
    Container sessions;

    // Ordered map of close times for sessions, grouped by the next multiple of close_interval
    using CloseTimes = std::map<std::chrono::steady_clock::time_point, std::unordered_set<Key, SessionKeyHash>>;
    CloseTimes close_time_buckets;

    constexpr static std::chrono::steady_clock::duration close_interval = std::chrono::milliseconds(1000);
    constexpr static std::chrono::nanoseconds::rep close_interval_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(close_interval).count();

    void scheduleCloseSession(NamedSessionData & session, std::unique_lock<std::mutex> &)
    {
        chassert(session.close_time_bucket == std::chrono::steady_clock::time_point{});

        const auto session_close_time = std::chrono::steady_clock::now() + session.timeout;
        const auto session_close_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(session_close_time.time_since_epoch()).count();
        const auto bucket_padding = close_interval - std::chrono::nanoseconds(session_close_time_ns % close_interval_ns);
        const auto close_time_bucket = session_close_time + bucket_padding;

        session.close_time_bucket = close_time_bucket;
        auto & bucket_sessions = close_time_buckets[close_time_bucket];
        bucket_sessions.insert(session.key);

        LOG_TEST(log, "Schedule closing session with session_id: {}, user_id: {}",
            session.key.second, toString(session.key.first));
    }

    void cleanThread()
    {
        setThreadName("SessionCleaner");
        std::unique_lock lock{mutex};
        while (!quit)
        {
            auto closed_sessions = closeSessions(lock);
            lock.unlock();
            closed_sessions.clear();
            lock.lock();
            if (cond.wait_for(lock, close_interval, [this]() -> bool { return quit; }))
                break;
        }
    }

    std::vector<std::shared_ptr<NamedSessionData>> closeSessions(std::unique_lock<std::mutex> & lock)
    {
        std::vector<std::shared_ptr<NamedSessionData>> closed_sessions;
        const auto now = std::chrono::steady_clock::now();

        for (auto bucket_it = close_time_buckets.begin(); bucket_it != close_time_buckets.end(); bucket_it = close_time_buckets.erase(bucket_it))
        {
            const auto & [time_bucket, session_keys] = *bucket_it;
            if (time_bucket > now)
                break;

            for (const auto & key : session_keys)
            {
                const auto & session_it = sessions.find(key);

                if (session_it == sessions.end())
                    continue;

                const auto & session = session_it->second;

                if (session.use_count() != 1)
                {
                    /// We can get here only if the session is still in use somehow. But since we don't allow concurrent usage
                    /// of the same session, the only way we can get here is when the session was released, but the pointer
                    /// wasn't reset yet. And since the pointer is reset without a lock, it's technically possible to get
                    /// into a situation when refcount > 1. In this case, we want to delay closing the session, but set the
                    /// timeout to 0 explicitly. This should be a very rare situation, since in order for it to happen, we should
                    /// have a session timeout less than close_interval, and also be able to reach this code before
                    /// resetting a pointer.
                    LOG_TEST(log, "Delay closing session with session_id: {}, user_id: {}, refcount: {}",
                        key.second, toString(key.first), session.use_count());

                    session->timeout = std::chrono::steady_clock::duration{0};
                    session->close_time_bucket = std::chrono::steady_clock::time_point{};
                    scheduleCloseSession(*session, lock);
                    continue;
                }

                LOG_TRACE(log, "Close session with session_id: {}, user_id: {}", key.second, toString(key.first));
                closed_sessions.push_back(session);
                sessions.erase(session_it);
            }
        }

        return closed_sessions;
    }

    std::mutex mutex;
    std::condition_variable cond;
    ThreadFromGlobalPool thread;
    bool quit = false;

    LoggerPtr log = getLogger("NamedSessionsStorage");
};


void NamedSessionData::release()
{
    parent.releaseSession(*this);
}

void Session::shutdownNamedSessions()
{
    NamedSessionsStorage::instance().shutdown();
}

Session::Session(const ContextPtr & global_context_, ClientInfo::Interface interface_, bool is_secure, const std::string & certificate)
    : auth_id(UUIDHelpers::generateV4()),
      global_context(global_context_),
      log(getLogger(String{magic_enum::enum_name(interface_)} + "-Session-" + toString(auth_id)))
{
    prepared_client_info.emplace();
    prepared_client_info->interface = interface_;
    prepared_client_info->is_secure = is_secure;
    prepared_client_info->certificate = certificate;
}

Session::~Session()
{
    /// Early release a NamedSessionData.
    if (named_session)
        named_session->release();

    if (notified_session_log_about_login)
    {
        LOG_DEBUG(log, "{} Logout, user_id: {}", toString(auth_id), toString(*user_id));
        if (auto session_log = getSessionLog())
        {
            session_log->addLogOut(auth_id, user, user_authenticated_with, getClientInfo());
        }
    }
}

std::unordered_set<AuthenticationType> Session::getAuthenticationTypes(const String & user_name) const
{
    std::unordered_set<AuthenticationType> authentication_types;

    const auto user_to_query = global_context->getAccessControl().read<User>(user_name);

    for (const auto & authentication_method : user_to_query->authentication_methods)
    {
        authentication_types.insert(authentication_method.getType());
    }

    return authentication_types;
}

std::unordered_set<AuthenticationType> Session::getAuthenticationTypesOrLogInFailure(const String & user_name) const
{
    try
    {
        return getAuthenticationTypes(user_name);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "{} Authentication failed with error: {}", toString(auth_id), e.what());
        if (auto session_log = getSessionLog())
            session_log->addLoginFailure(auth_id, getClientInfo(), user_name, e);

        throw;
    }
}

void Session::authenticate(const String & user_name, const String & password, const Poco::Net::SocketAddress & address, const Strings & external_roles_)
{
    authenticate(BasicCredentials{user_name, password}, address, external_roles_);
}

void Session::authenticate(const Credentials & credentials_, const Poco::Net::SocketAddress & address_, const Strings & external_roles_)
{
    if (session_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "If there is a session context it must be created after authentication");

    if (session_tracker_handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session tracker handle was created before authentication finish");

    auto address = address_;
    if ((address == Poco::Net::SocketAddress{}) && (prepared_client_info->interface == ClientInfo::Interface::LOCAL))
        address = Poco::Net::SocketAddress{"127.0.0.1", 0};

    LOG_DEBUG(log, "Authenticating user '{}' from {}",
            credentials_.getUserName(), address.toString());

    try
    {
        auto auth_result =
            global_context->getAccessControl().authenticate(credentials_, address.host(), getClientInfo());
        user_id = auth_result.user_id;
        user_authenticated_with = auth_result.authentication_data;
        settings_from_auth_server = auth_result.settings;
        LOG_DEBUG(log, "{} Authenticated with global context as user {}",
                toString(auth_id), toString(*user_id));

        if (!external_roles_.empty() && global_context->getSettingsRef()[Setting::push_external_roles_in_interserver_queries])
        {
            external_roles = global_context->getAccessControl().find<Role>(external_roles_);

            LOG_DEBUG(log, "User {} has external_roles applied: [{}] ({})",
                      toString(*user_id), fmt::join(external_roles_, ", "), external_roles_.size());
        }
    }
    catch (const Exception & e)
    {
        onAuthenticationFailure(credentials_.getUserName(), address, e);
        throw;
    }

    prepared_client_info->current_user = credentials_.getUserName();
    prepared_client_info->current_address = std::make_shared<Poco::Net::SocketAddress>(address);
}

void Session::checkIfUserIsStillValid()
{
    if (const auto valid_until = user_authenticated_with.getValidUntil())
    {
        const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

        if (now > valid_until)
            throw Exception(ErrorCodes::USER_EXPIRED, "Authentication method used has expired");
    }
}

void Session::onAuthenticationFailure(const std::optional<String> & user_name, const Poco::Net::SocketAddress & address_, const Exception & e)
{
    LOG_DEBUG(log, "Authentication failed with error: {}", e.what());
    if (auto session_log = getSessionLog())
    {
        /// Add source address to the log
        auto info_for_log = *prepared_client_info;
        info_for_log.current_address = std::make_shared<Poco::Net::SocketAddress>(address_);
        session_log->addLoginFailure(auth_id, info_for_log, user_name, e);
    }
}

const ClientInfo & Session::getClientInfo() const
{
    return session_context ? session_context->getClientInfo() : *prepared_client_info;
}

void Session::setClientInfo(const ClientInfo & client_info)
{
    if (session_context)
        session_context->setClientInfo(client_info);
    else
        prepared_client_info = client_info;
}

void Session::setClientName(const String & client_name)
{
    if (session_context)
        session_context->setClientName(client_name);
    else
        prepared_client_info->client_name = client_name;
}

void Session::setClientInterface(ClientInfo::Interface interface)
{
    if (session_context)
        session_context->setClientInterface(interface);
    else
        prepared_client_info->interface = interface;
}

void Session::setClientVersion(UInt64 client_version_major, UInt64 client_version_minor, UInt64 client_version_patch, unsigned client_tcp_protocol_version)
{
    if (session_context)
    {
        session_context->setClientVersion(client_version_major, client_version_minor, client_version_patch, client_tcp_protocol_version);
    }
    else
    {
        prepared_client_info->client_version_major = client_version_major;
        prepared_client_info->client_version_minor = client_version_minor;
        prepared_client_info->client_version_patch = client_version_patch;
        prepared_client_info->client_tcp_protocol_version = client_tcp_protocol_version;
    }
}

void Session::setClientConnectionId(uint32_t connection_id)
{
    if (session_context)
        session_context->setClientConnectionId(connection_id);
    else
        prepared_client_info->connection_id = connection_id;
}

void Session::setHTTPClientInfo(const Poco::Net::HTTPRequest & request)
{
    if (session_context)
        session_context->setHTTPClientInfo(request);
    else
        prepared_client_info->setFromHTTPRequest(request);
}

void Session::setForwardedFor(const String & forwarded_for)
{
    if (session_context)
        session_context->setForwardedFor(forwarded_for);
    else
        prepared_client_info->forwarded_for = forwarded_for;
}

void Session::setQuotaClientKey(const String & quota_key)
{
    if (session_context)
        session_context->setQuotaClientKey(quota_key);
    else
        prepared_client_info->quota_key = quota_key;
}

void Session::setConnectionClientVersion(UInt64 client_version_major, UInt64 client_version_minor, UInt64 client_version_patch, unsigned client_tcp_protocol_version)
{
    if (session_context)
    {
        session_context->setConnectionClientVersion(client_version_major, client_version_minor, client_version_patch, client_tcp_protocol_version);
    }
    else
    {
        prepared_client_info->connection_client_version_major = client_version_major;
        prepared_client_info->connection_client_version_minor = client_version_minor;
        prepared_client_info->connection_client_version_patch = client_version_patch;
        prepared_client_info->connection_tcp_protocol_version = client_tcp_protocol_version;
    }
}

const OpenTelemetry::TracingContext & Session::getClientTraceContext() const
{
    if (session_context)
        return session_context->getClientTraceContext();
    return prepared_client_info->client_trace_context;
}

OpenTelemetry::TracingContext & Session::getClientTraceContext()
{
    if (session_context)
        return session_context->getClientTraceContext();
    return prepared_client_info->client_trace_context;
}

ContextMutablePtr Session::makeSessionContext()
{
    if (session_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session context already exists");
    if (query_context_created)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session context must be created before any query context");
    if (!user_id)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session context must be created after authentication");
    if (session_tracker_handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session tracker handle was created before making session");

    LOG_DEBUG(log, "Creating session context with user_id: {}",
            toString(*user_id));
    /// Make a new session context.
    ContextMutablePtr new_session_context;
    new_session_context = Context::createCopy(global_context);
    new_session_context->makeSessionContext();

    /// Copy prepared client info to the new session context.
    new_session_context->setClientInfo(*prepared_client_info);
    prepared_client_info.reset();

    /// Set user information for the new context: current profiles, roles, access rights.
    new_session_context->setUser(*user_id, external_roles);

    /// Session context is ready.
    session_context = new_session_context;
    user = session_context->getUser();

    session_tracker_handle = session_context->getSessionTracker().trackSession(
        *user_id,
        {},
        session_context->getSettingsRef()[Setting::max_sessions_for_user]);

    // Use QUERY source as for SET query for a session
    session_context->checkSettingsConstraints(settings_from_auth_server, SettingSource::QUERY);
    session_context->applySettingsChanges(settings_from_auth_server);

    recordLoginSuccess(session_context);

    return session_context;
}

ContextMutablePtr Session::makeSessionContext(const String & session_name_, std::chrono::steady_clock::duration timeout_, bool session_check_)
{
    if (session_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session context already exists");
    if (query_context_created)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session context must be created before any query context");
    if (!user_id)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session context must be created after authentication");
    if (session_tracker_handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session tracker handle was created before making session");

    LOG_DEBUG(log, "Creating named session context with name: {}, user_id: {}",
            session_name_, toString(*user_id));

    /// Make a new session context OR
    /// if the `session_id` and `user_id` were used before then just get a previously created session context.
    std::shared_ptr<NamedSessionData> new_named_session;
    bool new_named_session_created = false;
    std::tie(new_named_session, new_named_session_created)
        = NamedSessionsStorage::instance().acquireSession(global_context, *user_id, session_name_, timeout_, session_check_);

    auto new_session_context = new_named_session->context;
    new_session_context->makeSessionContext();

    /// Copy prepared client info to the session context, no matter it's been just created or not.
    /// If we continue using a previously created session context found by session ID
    /// it's necessary to replace the client info in it anyway, because it contains actual connection information (client address, etc.)
    new_session_context->setClientInfo(*prepared_client_info);
    prepared_client_info.reset();

    auto access = new_session_context->getAccess();
    UInt64 max_sessions_for_user = 0;
    /// Set user information for the new context: current profiles, roles, access rights.
    if (!access->tryGetUser())
    {
        new_session_context->setUser(*user_id, external_roles);
        max_sessions_for_user = new_session_context->getSettingsRef()[Setting::max_sessions_for_user];
    }
    else
    {
        // Always get setting from profile
        // profile can be changed by ALTER PROFILE during single session
        auto settings = access->getDefaultSettings();
        const Field * max_session_for_user_field = settings.tryGet("max_sessions_for_user");
        if (max_session_for_user_field)
            max_sessions_for_user = max_session_for_user_field->safeGet<UInt64>();
    }

    /// Session context is ready.
    session_context = std::move(new_session_context);
    named_session = new_named_session;
    named_session_created = new_named_session_created;
    user = session_context->getUser();

    session_tracker_handle = session_context->getSessionTracker().trackSession(
        *user_id,
        { session_name_ },
        max_sessions_for_user);

    recordLoginSuccess(session_context);

    return session_context;
}

ContextMutablePtr Session::makeQueryContext(const ClientInfo & query_client_info) const
{
    return makeQueryContextImpl(&query_client_info, nullptr);
}

ContextMutablePtr Session::makeQueryContext(ClientInfo && query_client_info) const
{
    return makeQueryContextImpl(nullptr, &query_client_info);
}

std::shared_ptr<SessionLog> Session::getSessionLog() const
{
    // take it from global context, since it outlives the Session and always available.
    // please note that server may have session_log disabled, hence this may return nullptr.
    return global_context->getSessionLog();
}

ContextMutablePtr Session::makeQueryContextImpl(const ClientInfo * client_info_to_copy, ClientInfo * client_info_to_move) const
{
    if (!user_id && getClientInfo().interface != ClientInfo::Interface::TCP_INTERSERVER)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query context must be created after authentication");

    /// We can create a query context either from a session context or from a global context.
    const bool from_session_context = static_cast<bool>(session_context);

    /// Create a new query context.
    ContextMutablePtr query_context = Context::createCopy(from_session_context ? session_context : global_context);
    query_context->makeQueryContext();

    if (auto query_context_user = query_context->getAccess()->tryGetUser())
    {
        LOG_TRACE(log, "{} Creating query context from {} context, user_id: {}, parent context user: {}",
                  toString(auth_id),
                  from_session_context ? "session" : "global",
                  toString(*user_id),
                  query_context_user->getName());
    }

    /// Copy the specified client info to the new query context.
    if (client_info_to_move)
        query_context->setClientInfo(*client_info_to_move);
    else if (client_info_to_copy && (client_info_to_copy != &getClientInfo()))
        query_context->setClientInfo(*client_info_to_copy);

    /// Copy current user's name and address if it was authenticated after query_client_info was initialized.
    if (prepared_client_info && !prepared_client_info->current_user.empty())
    {
        query_context->setCurrentUserName(prepared_client_info->current_user);
        query_context->setCurrentAddress(*prepared_client_info->current_address);
    }

    /// Set parameters of initial query.
    if (query_context->getClientInfo().query_kind == ClientInfo::QueryKind::NO_QUERY)
        query_context->setQueryKind(ClientInfo::QueryKind::INITIAL_QUERY);

    if (query_context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        query_context->setInitialUserName(query_context->getClientInfo().current_user);
        query_context->setInitialAddress(*query_context->getClientInfo().current_address);
    }

    /// Set user information for the new context: current profiles, roles, access rights.
    if (user_id && !query_context->getAccess()->tryGetUser())
        query_context->setUser(*user_id, external_roles);

    /// Query context is ready.
    query_context_created = true;
    if (user_id)
        user = query_context->getUser();

    /// Interserver does not create session context
    recordLoginSuccess(query_context);

    return query_context;
}


void Session::recordLoginSuccess(ContextPtr login_context) const
{
    if (notified_session_log_about_login)
        return;

    if (!login_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session or query context must be created");

    if (auto session_log = getSessionLog())
    {
        const auto & settings   = login_context->getSettingsRef();
        const auto access       = login_context->getAccess();

        session_log->addLoginSuccess(auth_id,
                                     named_session ? named_session->key.second : "",
                                     settings,
                                     access->getAccess(),
                                     getClientInfo(),
                                     user,
                                     user_authenticated_with);
    }

    notified_session_log_about_login = true;
}


void Session::releaseSessionID()
{
    if (!named_session)
        return;

    prepared_client_info = getClientInfo();
    session_context.reset();

    named_session->release();
    named_session = nullptr;
}

void Session::closeSession(const String & session_id)
{
    if (!user_id)   /// User was not authenticated
        return;

    /// named_session may be not set due to an early exception
    if (!named_session)
        return;

    NamedSessionsStorage::instance().releaseAndCloseSession(*user_id, session_id, named_session);
}

}
