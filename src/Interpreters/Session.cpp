#include <Interpreters/Session.h>

#include <Access/AccessControl.h>
#include <Access/Credentials.h>
#include <Access/ContextAccess.h>
#include <Access/User.h>
#include <base/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>
#include <Interpreters/SessionLog.h>

#include <magic_enum.hpp>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SESSION_NOT_FOUND;
    extern const int SESSION_IS_LOCKED;
}


class NamedSessionsStorage;

/// User ID and session identifier. Named sessions are local to users.
using NamedSessionKey = std::pair<UUID, String>;

/// Named sessions. The user could specify session identifier to reuse settings and temporary tables in subsequent requests.
struct NamedSessionData
{
    NamedSessionKey key;
    UInt64 close_cycle = 0;
    ContextMutablePtr context;
    std::chrono::steady_clock::duration timeout;
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
                throw Exception("Session not found.", ErrorCodes::SESSION_NOT_FOUND);

            /// Create a new session from current context.
            auto context = Context::createCopy(global_context);
            it = sessions.insert(std::make_pair(key, std::make_shared<NamedSessionData>(key, context, timeout, *this))).first;
            const auto & session = it->second;

            if (!thread.joinable())
                thread = ThreadFromGlobalPool{&NamedSessionsStorage::cleanThread, this};

            return {session, true};
        }
        else
        {
            /// Use existing session.
            const auto & session = it->second;

            if (!session.unique())
                throw Exception("Session is locked by a concurrent client.", ErrorCodes::SESSION_IS_LOCKED);
            return {session, false};
        }
    }

    void releaseSession(NamedSessionData & session)
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
    using Container = std::unordered_map<Key, std::shared_ptr<NamedSessionData>, SessionKeyHash>;
    using CloseTimes = std::deque<std::vector<Key>>;
    Container sessions;
    CloseTimes close_times;
    std::chrono::steady_clock::duration close_interval = std::chrono::seconds(1);
    std::chrono::steady_clock::time_point close_cycle_time = std::chrono::steady_clock::now();
    UInt64 close_cycle = 0;

    void scheduleCloseSession(NamedSessionData & session, std::unique_lock<std::mutex> &)
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
        while (!quit)
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
    ThreadFromGlobalPool thread;
    bool quit = false;
};


void NamedSessionData::release()
{
    parent.releaseSession(*this);
}

void Session::shutdownNamedSessions()
{
    NamedSessionsStorage::instance().shutdown();
}

Session::Session(const ContextPtr & global_context_, ClientInfo::Interface interface_, bool is_secure)
    : auth_id(UUIDHelpers::generateV4()),
      global_context(global_context_),
      interface(interface_),
      log(&Poco::Logger::get(String{magic_enum::enum_name(interface_)} + "-Session"))
{
    prepared_client_info.emplace();
    prepared_client_info->interface = interface_;
    prepared_client_info->is_secure = is_secure;
}

Session::~Session()
{
    LOG_DEBUG(log, "{} Destroying {} of user {}",
        toString(auth_id),
        (named_session ? "named session '" + named_session->key.second + "'" : "unnamed session"),
        (user_id ? toString(*user_id) : "<EMPTY>")
    );

    /// Early release a NamedSessionData.
    if (named_session)
        named_session->release();

    if (notified_session_log_about_login)
    {
        if (auto session_log = getSessionLog(); session_log && user)
            session_log->addLogOut(auth_id, user->getName(), getClientInfo());
    }
}

AuthenticationType Session::getAuthenticationType(const String & user_name) const
{
    return global_context->getAccessControl().read<User>(user_name)->auth_data.getType();
}

AuthenticationType Session::getAuthenticationTypeOrLogInFailure(const String & user_name) const
{
    try
    {
        return getAuthenticationType(user_name);
    }
    catch (const Exception & e)
    {
        if (auto session_log = getSessionLog())
            session_log->addLoginFailure(auth_id, getClientInfo(), user_name, e);

        throw;
    }
}

void Session::authenticate(const String & user_name, const String & password, const Poco::Net::SocketAddress & address)
{
    authenticate(BasicCredentials{user_name, password}, address);
}

void Session::authenticate(const Credentials & credentials_, const Poco::Net::SocketAddress & address_)
{
    if (session_context)
        throw Exception("If there is a session context it must be created after authentication", ErrorCodes::LOGICAL_ERROR);

    auto address = address_;
    if ((address == Poco::Net::SocketAddress{}) && (prepared_client_info->interface == ClientInfo::Interface::LOCAL))
        address = Poco::Net::SocketAddress{"127.0.0.1", 0};

    LOG_DEBUG(log, "{} Authenticating user '{}' from {}",
            toString(auth_id), credentials_.getUserName(), address.toString());

    try
    {
        user_id = global_context->getAccessControl().authenticate(credentials_, address.host());
        LOG_DEBUG(log, "{} Authenticated with global context as user {}",
                toString(auth_id), user_id ? toString(*user_id) : "<EMPTY>");
    }
    catch (const Exception & e)
    {
        LOG_DEBUG(log, "{} Authentication failed with error: {}", toString(auth_id), e.what());
        if (auto session_log = getSessionLog())
            session_log->addLoginFailure(auth_id, *prepared_client_info, credentials_.getUserName(), e);
        throw;
    }

    prepared_client_info->current_user = credentials_.getUserName();
    prepared_client_info->current_address = address;
}

ClientInfo & Session::getClientInfo()
{
    return session_context ? session_context->getClientInfo() : *prepared_client_info;
}

const ClientInfo & Session::getClientInfo() const
{
    return session_context ? session_context->getClientInfo() : *prepared_client_info;
}

ContextMutablePtr Session::makeSessionContext()
{
    if (session_context)
        throw Exception("Session context already exists", ErrorCodes::LOGICAL_ERROR);
    if (query_context_created)
        throw Exception("Session context must be created before any query context", ErrorCodes::LOGICAL_ERROR);

    LOG_DEBUG(log, "{} Creating session context with user_id: {}",
            toString(auth_id), user_id ? toString(*user_id) : "<EMPTY>");
    /// Make a new session context.
    ContextMutablePtr new_session_context;
    new_session_context = Context::createCopy(global_context);
    new_session_context->makeSessionContext();

    /// Copy prepared client info to the new session context.
    auto & res_client_info = new_session_context->getClientInfo();
    res_client_info = std::move(prepared_client_info).value();
    prepared_client_info.reset();

    /// Set user information for the new context: current profiles, roles, access rights.
    if (user_id)
        new_session_context->setUser(*user_id);

    /// Session context is ready.
    session_context = new_session_context;
    user = session_context->getUser();

    return session_context;
}

ContextMutablePtr Session::makeSessionContext(const String & session_name_, std::chrono::steady_clock::duration timeout_, bool session_check_)
{
    if (session_context)
        throw Exception("Session context already exists", ErrorCodes::LOGICAL_ERROR);
    if (query_context_created)
        throw Exception("Session context must be created before any query context", ErrorCodes::LOGICAL_ERROR);

    LOG_DEBUG(log, "{} Creating named session context with name: {}, user_id: {}",
              toString(auth_id), session_name_, user_id ? toString(*user_id) : "<EMPTY>");

    /// Make a new session context OR
    /// if the `session_id` and `user_id` were used before then just get a previously created session context.
    std::shared_ptr<NamedSessionData> new_named_session;
    bool new_named_session_created = false;
    std::tie(new_named_session, new_named_session_created)
        = NamedSessionsStorage::instance().acquireSession(global_context, user_id.value_or(UUID{}), session_name_, timeout_, session_check_);

    auto new_session_context = new_named_session->context;
    new_session_context->makeSessionContext();

    /// Copy prepared client info to the session context, no matter it's been just created or not.
    /// If we continue using a previously created session context found by session ID
    /// it's necessary to replace the client info in it anyway, because it contains actual connection information (client address, etc.)
    auto & res_client_info = new_session_context->getClientInfo();
    res_client_info = std::move(prepared_client_info).value();
    prepared_client_info.reset();

    /// Set user information for the new context: current profiles, roles, access rights.
    if (user_id && !new_session_context->getAccess()->tryGetUser())
        new_session_context->setUser(*user_id);

    /// Session context is ready.
    session_context = std::move(new_session_context);
    named_session = new_named_session;
    named_session_created = new_named_session_created;
    user = session_context->getUser();

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
    /// For the LOCAL interface we don't send events to the session log
    /// because the LOCAL interface is internal, it does nothing with networking.
    if (interface == ClientInfo::Interface::LOCAL)
        return nullptr;

    // take it from global context, since it outlives the Session and always available.
    // please note that server may have session_log disabled, hence this may return nullptr.
    return global_context->getSessionLog();
}

ContextMutablePtr Session::makeQueryContextImpl(const ClientInfo * client_info_to_copy, ClientInfo * client_info_to_move) const
{
    /// We can create a query context either from a session context or from a global context.
    bool from_session_context = static_cast<bool>(session_context);

    /// Create a new query context.
    ContextMutablePtr query_context = Context::createCopy(from_session_context ? session_context : global_context);
    query_context->makeQueryContext();

    if (auto query_context_user = query_context->getAccess()->tryGetUser())
    {
        LOG_DEBUG(log, "{} Creating query context from {} context, user_id: {}, parent context user: {}",
                  toString(auth_id),
                  from_session_context ? "session" : "global",
                  toString(*user_id),
                  query_context_user->getName());
    }

    /// Copy the specified client info to the new query context.
    auto & res_client_info = query_context->getClientInfo();
    if (client_info_to_move)
        res_client_info = std::move(*client_info_to_move);
    else if (client_info_to_copy && (client_info_to_copy != &getClientInfo()))
        res_client_info = *client_info_to_copy;

    /// Copy current user's name and address if it was authenticated after query_client_info was initialized.
    if (prepared_client_info && !prepared_client_info->current_user.empty())
    {
        res_client_info.current_user = prepared_client_info->current_user;
        res_client_info.current_address = prepared_client_info->current_address;
    }

    /// Set parameters of initial query.
    if (res_client_info.query_kind == ClientInfo::QueryKind::NO_QUERY)
        res_client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;

    if (res_client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        res_client_info.initial_user = res_client_info.current_user;
        res_client_info.initial_address = res_client_info.current_address;
    }

    /// Sets that row policies of the initial user should be used too.
    query_context->enableRowPoliciesOfInitialUser();

    /// Set user information for the new context: current profiles, roles, access rights.
    if (user_id && !query_context->getAccess()->tryGetUser())
        query_context->setUser(*user_id);

    /// Query context is ready.
    query_context_created = true;
    if (user_id)
        user = query_context->getUser();

    if (!notified_session_log_about_login)
    {
        if (auto session_log = getSessionLog(); user && user_id && session_log)
        {
            session_log->addLoginSuccess(
                    auth_id,
                    named_session ? std::optional<std::string>(named_session->key.second) : std::nullopt,
                    *query_context);

            notified_session_log_about_login = true;
        }
    }

    return query_context;
}


void Session::releaseSessionID()
{
    if (!named_session)
        return;
    named_session->release();
    named_session = nullptr;
}

}

