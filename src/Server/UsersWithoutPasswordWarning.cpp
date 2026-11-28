#include <Server/UsersWithoutPasswordWarning.h>

#include <Access/AccessControl.h>
#include <Access/IAccessStorage.h>
#include <Access/User.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>

#include <Poco/Net/IPAddress.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <chrono>
#include <mutex>
#include <optional>
#include <unordered_map>


namespace DB
{

namespace
{

/// The longest delay of the re-check at the expiry of a reported login. A later expiry is re-scheduled from there,
/// which also copes with a changed system clock.
constexpr time_t max_expiry_recheck_delay_seconds = 24 * 60 * 60;

}

struct UsersWithoutPasswordWarning::State : public std::enable_shared_from_this<State>
{
    explicit State(ContextPtr global_context_)
        : global_context(global_context_)
    {
    }

    /// Re-checks if `changed`, or if the previous check failed. Never throws: a failed check is retried on the next call.
    void recheck(bool changed) TSA_REQUIRES(mutex);

    void scheduleRecheck(const Context & context, time_t at, time_t now) TSA_REQUIRES(mutex);

    const ContextWeakPtr global_context;
    const LoggerPtr log = getLogger("UsersWithoutPasswordWarning");

    std::mutex mutex;
    bool shut_down TSA_GUARDED_BY(mutex) = false;

    /// The non-loopback listeners.
    std::vector<Listener> listeners TSA_GUARDED_BY(mutex);

    /// The text of the warning that was passed to the context last; nullopt if it was removed.
    /// The warning is only touched when this text changes, so a `TRUNCATE TABLE system.warnings` lasts until then.
    std::optional<String> published_text TSA_GUARDED_BY(mutex);

    /// Whether the previous check failed.
    bool pending TSA_GUARDED_BY(mutex) = false;

    /// The results of the previous check by user name. User entities are immutable, so the result for the same entity
    /// and settings is reused: a `bcrypt_password` is expensive to check, and a check runs on every change of any user.
    struct CheckedUser
    {
        UserPtr user;
        bool allow_no_password = false;
        bool allow_plaintext_password = false;
        bool accepts_empty_password = false;
        /// When the login with an empty password expires, 0 if never.
        time_t valid_until = 0;
    };
    std::unordered_map<String, CheckedUser> checked_users TSA_GUARDED_BY(mutex);

    /// Re-checks at the expiry of a reported login. Created on first use.
    BackgroundSchedulePoolTaskHolder expiry_task TSA_GUARDED_BY(mutex);
};


void UsersWithoutPasswordWarning::State::recheck(bool changed)
{
    if (shut_down || (!changed && !pending))
        return;

    auto context = global_context.lock();
    if (!context)
        return;

    try
    {
        const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::optional<PreformattedMessage> warning;
        std::unordered_map<String, CheckedUser> new_checked_users;
        time_t next_expiry = 0;

        if (!listeners.empty())
        {
            /// `default` is the well-known name to try, the default session users are what clients get without a user name.
            Strings user_names{"default"};
            for (const auto & listener : listeners)
            {
                if (!listener.default_session_user.empty()
                    && std::ranges::find(user_names, listener.default_session_user) == user_names.end())
                    user_names.push_back(listener.default_session_user);
            }

            const auto & access_control = context->getAccessControl();
            const bool allow_no_password = access_control.isNoPasswordAllowed();
            const bool allow_plaintext_password = access_control.isPlaintextPasswordAllowed();

            Strings reported_users;
            for (const auto & user_name : user_names)
            {
                /// A passwordless user restricted by `networks` / `HOST` (e.g. to localhost) is a common, deliberate setup.
                auto user = access_control.tryRead<User>(user_name);
                if (!user || !user->allowed_client_hosts.containsAnyHost())
                    continue;

                CheckedUser checked;
                auto it = checked_users.find(user_name);
                if (it != checked_users.end() && it->second.user == user && it->second.allow_no_password == allow_no_password
                    && it->second.allow_plaintext_password == allow_plaintext_password)
                {
                    checked = it->second;
                }
                else
                {
                    checked.user = user;
                    checked.allow_no_password = allow_no_password;
                    checked.allow_plaintext_password = allow_plaintext_password;
                    checked.accepts_empty_password = IAccessStorage::acceptsEmptyPassword(
                        *user, allow_no_password, allow_plaintext_password, now, &checked.valid_until);
                }
                new_checked_users.emplace(user_name, checked);

                /// Time only makes authentication methods expire: a login that has failed keeps failing for the same entity.
                if (!checked.accepts_empty_password || (checked.valid_until && now > checked.valid_until))
                    continue;

                reported_users.push_back(quoteString(user_name));
                if (checked.valid_until && (!next_expiry || checked.valid_until < next_expiry))
                    next_expiry = checked.valid_until;
            }

            if (!reported_users.empty())
            {
                warning = PreformattedMessage::create(
                    "The server listens on {}, and these users can connect from any host without a password: {}. "
                    "Only `default` and the users of connections without a user name are checked. "
                    "Set a password for them or restrict the hosts they can connect from (`<networks>` in users.xml, `HOST` in SQL).",
                    quoteString(listeners.front().host),
                    fmt::format("{}", fmt::join(reported_users, ", ")));
            }
        }

        std::optional<String> text;
        if (warning)
            text = warning->text;
        if (text != published_text)
        {
            context->addOrUpdateWarningMessage(Context::WarningType::USERS_WITHOUT_PASSWORD, warning);
            published_text = std::move(text);
        }

        checked_users = std::move(new_checked_users);
        if (next_expiry)
            scheduleRecheck(*context, next_expiry, now);
        pending = false;
    }
    catch (...)
    {
        /// Keep the previous state of the warning. Logged once per series of failures.
        if (!pending)
            tryLogCurrentException(log, "Cannot check for users without a password, will retry on the next change of access entities");
        pending = true;
    }
}


void UsersWithoutPasswordWarning::State::scheduleRecheck(const Context & context, time_t at, time_t now)
{
    if (!expiry_task)
    {
        expiry_task = context.getSchedulePool()->createTask(
            StorageID::createEmpty(),
            "UsersWithoutPasswordWarning",
            [weak_state = weak_from_this()]
            {
                if (auto locked_state = weak_state.lock())
                {
                    std::lock_guard lock(locked_state->mutex);
                    locked_state->recheck(/* changed = */ true);
                }
            });
    }

    /// A login is accepted while `now <= valid_until`.
    const time_t delay_seconds = std::clamp<time_t>(at + 1 - now, 1, max_expiry_recheck_delay_seconds);
    expiry_task->scheduleAfter(static_cast<size_t>(delay_seconds) * 1000);
}


UsersWithoutPasswordWarning::UsersWithoutPasswordWarning(ContextPtr global_context)
    : state(std::make_shared<State>(global_context))
{
    /// `AccessChangesNotifier::sendNotifications` calls the handlers outside of its lock, so the handler can still be called
    /// after the unsubscription: it holds the state through a `weak_ptr`.
    subscription = global_context->getAccessControl().subscribeForChanges<User>(
        [weak_state = std::weak_ptr<State>(state)](const std::vector<AccessChangesNotifier::Change> & changes)
        {
            if (auto locked_state = weak_state.lock())
            {
                std::lock_guard lock(locked_state->mutex);
                /// By-type handlers are also called with an empty batch, which only retries a failed check.
                locked_state->recheck(/* changed = */ !changes.empty());
            }
        });
}


UsersWithoutPasswordWarning::~UsersWithoutPasswordWarning()
{
    subscription.reset();

    BackgroundSchedulePoolTaskHolder expiry_task;
    {
        std::lock_guard lock(state->mutex);
        state->shut_down = true;
        expiry_task = std::move(state->expiry_task);
    }
    /// The task holder waits for a running re-check, which takes the lock, so it is destroyed after the lock is released.
}


void UsersWithoutPasswordWarning::setListeners(const std::vector<Listener> & listeners)
{
    try
    {
        std::vector<Listener> non_loopback_listeners;
        for (const auto & listener : listeners)
        {
            if (!isLoopbackHost(listener.host))
                non_loopback_listeners.push_back(listener);
        }

        std::lock_guard lock(state->mutex);
        state->listeners = std::move(non_loopback_listeners);
        state->recheck(/* changed = */ true);
    }
    catch (...)
    {
        tryLogCurrentException(state->log, "Cannot update the listeners");
    }
}


bool UsersWithoutPasswordWarning::isLoopbackHost(const String & host)
{
    Poco::Net::IPAddress address;
    if (!Poco::Net::IPAddress::tryParse(host, address))
        return equalsCaseInsensitive(host, "localhost");

    /// `::ffff:127.0.0.1` is the IPv4 loopback address of a dual-stack socket.
    if (address.family() == Poco::Net::IPAddress::IPv6 && address.isIPv4Mapped())
        address = Poco::Net::IPAddress(reinterpret_cast<const char *>(address.addr()) + 12, 4);

    return address.isLoopback();
}

}
