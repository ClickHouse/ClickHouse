#include <Common/NameResolutionFilter.h>

#include <Common/Exception.h>
#include <Common/LoggingHelpers.h>
#include <Common/ProfileEvents.h>
#include <Common/RemoteHostFilter.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <base/defines.h>
#include <base/getFQDNOrHostName.h>
#include <base/scope_guard.h>

#include <Poco/Net/DNS.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <atomic>
#include <unordered_set>

#if defined(OS_LINUX)
#    include <netdb.h>
#    include <dlfcn.h>
#endif


namespace ProfileEvents
{
    extern const Event DNSRequestNotAllowed;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNACCEPTABLE_URL;
}

namespace
{

struct State
{
    std::atomic_bool enabled = false;

    /// A separate instance from the one in `Context`: the state outlives the global context, because library
    /// threads may resolve names during shutdown.
    RemoteHostFilter allowed_hosts;

    SharedMutex internal_hosts_mutex;
    std::unordered_set<std::string> internal_hosts TSA_GUARDED_BY(internal_hosts_mutex);
};

State & state()
{
    /// Never destroyed, for the same reason.
    static State * const instance = new State;
    return *instance;
}

/// The depth of `InternalScope`s of the current thread.
thread_local size_t internal_scope_depth = 0;

/// Set while the filter itself runs: logging a rejection must not recurse into the filter if a log channel
/// resolves a name.
thread_local bool inside_filter = false;

/// `[::1]` is `::1`; host names are case-insensitive.
std::string normalizeHost(std::string_view host)
{
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']')
        host = host.substr(1, host.size() - 2);

    std::string result(host);
    for (auto & c : result)
        c = toLowerIfAlphaASCII(c);
    return result;
}

bool isAllowedImpl(std::string_view host)
{
    auto & s = state();
    if (!s.enabled.load(std::memory_order_relaxed) || internal_scope_depth || inside_filter)
        return true;

    inside_filter = true;
    SCOPE_EXIT({ inside_filter = false; });

    /// The entries of `remote_url_allow_hosts` are compared as they are written, as in `RemoteHostFilter`,
    /// and also with the lower-cased host.
    const std::string original(host);
    const std::string normalized = normalizeHost(host);

    {
        SharedLockGuard lock(s.internal_hosts_mutex);
        if (s.internal_hosts.contains(normalized))
            return true;
    }

    return s.allowed_hosts.checkHostName(original) || s.allowed_hosts.checkHostName(normalized);
}

}

void NameResolutionFilter::setValuesFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    auto & s = state();
    s.allowed_hosts.setValuesFromConfig(config);
    const bool enable = config.has("remote_url_allow_hosts") && config.getBool("remote_url_allow_hosts.check_name_resolution", false);

    if (enable)
    {
        /// The server resolves its own name, e.g. for `getFQDNOrHostName`, which is computed once, here if not before.
        addInternalHost(Poco::Net::DNS::hostName());
        addInternalHost(getFQDNOrHostName());
    }

    s.enabled = enable;
}

bool NameResolutionFilter::isEnabled()
{
    return state().enabled.load(std::memory_order_relaxed);
}

void NameResolutionFilter::addInternalHost(std::string_view host)
{
    if (host.empty())
        return;

    auto normalized = normalizeHost(host);
    auto & s = state();

    {
        SharedLockGuard lock(s.internal_hosts_mutex);
        if (s.internal_hosts.contains(normalized))
            return;
    }

    std::lock_guard lock(s.internal_hosts_mutex);
    s.internal_hosts.insert(std::move(normalized));
}

bool NameResolutionFilter::isAllowed(std::string_view host)
{
    if (isAllowedImpl(host))
        return true;

    ProfileEvents::increment(ProfileEvents::DNSRequestNotAllowed);

    inside_filter = true;
    SCOPE_EXIT({ inside_filter = false; });
    LOG_WARNING(LogFrequencyLimiter(getLogger("NameResolutionFilter"), 10),
        "Resolution of the host name '{}' is not allowed, see <remote_url_allow_hosts>", host);

    return false;
}

void NameResolutionFilter::check(std::string_view host)
{
    if (isAllowedImpl(host))
        return;

    ProfileEvents::increment(ProfileEvents::DNSRequestNotAllowed);
    throw Exception(ErrorCodes::UNACCEPTABLE_URL,
        "Resolution of the host name '{}' is not allowed in configuration file, see <remote_url_allow_hosts>", host);
}

NameResolutionFilter::InternalScope::InternalScope()
{
    ++internal_scope_depth;
}

NameResolutionFilter::InternalScope::~InternalScope()
{
    --internal_scope_depth;
}

}


#if defined(OS_LINUX)

/// Interposes `getaddrinfo` of the C library, which is the forward name resolution of every library linked into
/// the server: the definition in the executable takes precedence over the one in `libc.so`, and the original is
/// reached through `dlsym(RTLD_NEXT)`. A sanitizer runtime intercepts `getaddrinfo` in the same way; it is reached
/// through its interceptor, which keeps the interception in place. With musl, the C library is linked statically
/// and its `getaddrinfo` is compiled as `__musl_getaddrinfo` (see `contrib/musl-cmake`).
///
/// The other forward resolution functions are not used: `gethostbyname` and the like are trapped in `base/harmful`.
/// `c-ares` sends DNS queries by itself, so gRPC is built with its native resolver, which uses `getaddrinfo`.

namespace
{

using GetAddrInfo = int (*)(const char *, const char *, const struct addrinfo *, struct addrinfo **);

}

#if defined(USE_MUSL)
extern "C" int __musl_getaddrinfo(const char * node, const char * service, const struct addrinfo * hints, struct addrinfo ** res); // NOLINT(bugprone-reserved-identifier)
#elif defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER)
extern "C" int __interceptor_getaddrinfo(const char * node, const char * service, const struct addrinfo * hints, struct addrinfo ** res); // NOLINT(bugprone-reserved-identifier)
#endif

namespace
{

GetAddrInfo originalGetAddrInfo()
{
#if defined(USE_MUSL)
    return &__musl_getaddrinfo;
#elif defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER)
    return &__interceptor_getaddrinfo;
#else
    static const GetAddrInfo original = []
    {
        auto * function = reinterpret_cast<GetAddrInfo>(dlsym(RTLD_NEXT, "getaddrinfo"));
        if (!function)
            abort();
        return function;
    }();
    return original;
#endif
}

}

extern "C" int getaddrinfo(const char * node, const char * service, const struct addrinfo * hints, struct addrinfo ** res)
{
    if (node)
    {
        /// An exception must not leave through the frames of a C library; failing the resolution is the safe side.
        try
        {
            if (!DB::NameResolutionFilter::isAllowed(node))
                return EAI_NONAME;
        }
        catch (...) // Ok: the exception is turned into a failed resolution, it cannot propagate through C frames.
        {
            return EAI_FAIL;
        }
    }

    return originalGetAddrInfo()(node, service, hints, res);
}

#endif
