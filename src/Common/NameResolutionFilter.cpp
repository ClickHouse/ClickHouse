#include <Common/NameResolutionFilter.h>

#include <Common/Exception.h>
#include <Common/FiberLocal.h>
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
#include <charconv>
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
    /// Exempt in the resolver of the server itself.
    std::unordered_set<std::string> internal_hosts TSA_GUARDED_BY(internal_hosts_mutex);
    /// Exempt everywhere, as `host:port`.
    std::unordered_set<std::string> internal_endpoints TSA_GUARDED_BY(internal_hosts_mutex);
};

State & state()
{
    /// Never destroyed, for the same reason.
    static State * const instance = new State;
    return *instance;
}

/// The depth of `InternalScope`s of the current thread or fiber. It is not a plain `thread_local`: a scope may
/// be held across a suspension of a fiber (credential providers refresh inside fibers), and the fiber may resume
/// on another thread, which would leave the first thread exempted forever.
constinit FiberLocal<size_t, FiberLocalSlot::NAME_RESOLUTION_INTERNAL_SCOPE_DEPTH> internal_scope_depth;

/// Set while the filter itself runs: logging a rejection must not recurse into the filter if a log channel
/// resolves a name.
constinit FiberLocal<bool, FiberLocalSlot::NAME_RESOLUTION_INSIDE_FILTER> inside_filter;

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

std::string endpointKey(std::string_view normalized_host, std::string_view service)
{
    std::string key;
    key.reserve(normalized_host.size() + 1 + service.size());
    key += normalized_host;
    key += ':';
    key += service;
    return key;
}

enum class Caller : uint8_t
{
    /// `DNSResolver` and the HTTP connection pool: the host comes from the code of the server.
    Server,
    /// `getaddrinfo` called by a library: the host may have been discovered by the library.
    Library,
};

bool isAllowedImpl(std::string_view host, std::string_view service, Caller caller)
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
        if (caller == Caller::Server && s.internal_hosts.contains(normalized))
            return true;
        if (!service.empty() && s.internal_endpoints.contains(endpointKey(normalized, service)))
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
        InternalScope internal_scope;
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

void NameResolutionFilter::addInternalEndpoint(std::string_view host, UInt16 port)
{
    if (host.empty())
        return;

    addInternalHost(host);

    auto key = endpointKey(normalizeHost(host), std::to_string(port));
    auto & s = state();

    {
        SharedLockGuard lock(s.internal_hosts_mutex);
        if (s.internal_endpoints.contains(key))
            return;
    }

    std::lock_guard lock(s.internal_hosts_mutex);
    s.internal_endpoints.insert(std::move(key));
}

void NameResolutionFilter::addInternalEndpoint(std::string_view host_and_port)
{
    /// An endpoint without a numeric port cannot be registered: it stays subject to `remote_url_allow_hosts`.
    const auto colon = host_and_port.rfind(':');
    if (colon == std::string_view::npos)
        return;

    UInt16 port = 0;
    const auto port_string = host_and_port.substr(colon + 1);
    const auto [end, error] = std::from_chars(port_string.data(), port_string.data() + port_string.size(), port);
    if (error != std::errc() || end != port_string.data() + port_string.size())
        return;

    addInternalEndpoint(host_and_port.substr(0, colon), port);
}

bool NameResolutionFilter::isAllowed(std::string_view host, std::string_view service)
{
    if (isAllowedImpl(host, service, Caller::Library))
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
    if (isAllowedImpl(host, {}, Caller::Server))
        return;

    ProfileEvents::increment(ProfileEvents::DNSRequestNotAllowed);
    throw Exception(ErrorCodes::UNACCEPTABLE_URL,
        "Resolution of the host name '{}' is not allowed in configuration file, see <remote_url_allow_hosts>", host);
}

NameResolutionFilter::InternalScope::InternalScope()
{
    internal_scope_depth = internal_scope_depth + 1;
}

NameResolutionFilter::InternalScope::~InternalScope()
{
    internal_scope_depth = internal_scope_depth - 1;
}

}


#if defined(OS_LINUX)

/// Interposes `getaddrinfo` of the C library, which is the forward name resolution of every library linked into
/// the server: the definition in the executable takes precedence over the one in `libc.so`, and the original is
/// reached through `dlsym(RTLD_NEXT)`. A sanitizer runtime intercepts `getaddrinfo` in the same way; it is reached
/// through its interceptor, which keeps the interception in place, with glibc and with musl. With musl and no
/// sanitizer, the C library is linked statically and its `getaddrinfo` is compiled as `__musl_getaddrinfo`
/// (see `contrib/musl-cmake`).
///
/// The other forward resolution functions are not used: `gethostbyname` and the like are trapped in `base/harmful`.
/// `c-ares` sends DNS queries by itself, so gRPC is built with its native resolver, which uses `getaddrinfo`.

namespace
{

using GetAddrInfo = int (*)(const char *, const char *, const struct addrinfo *, struct addrinfo **);

}

/// The names are reserved identifiers, but they are the names that musl and the sanitizer runtime define.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER)
extern "C" int __interceptor_getaddrinfo(const char * node, const char * service, const struct addrinfo * hints, struct addrinfo ** res); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
#elif defined(USE_MUSL)
extern "C" int __musl_getaddrinfo(const char * node, const char * service, const struct addrinfo * hints, struct addrinfo ** res); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
#endif
#pragma clang diagnostic pop

namespace
{

GetAddrInfo originalGetAddrInfo()
{
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER)
    /// With musl too: the build renames the `getaddrinfo` of musl to `__real_getaddrinfo`, which is what the
    /// interceptor calls (see `contrib/compiler-rt-cmake`). Calling musl directly would skip the interceptor,
    /// and MSan would see the result as uninitialized.
    return &__interceptor_getaddrinfo;
#elif defined(USE_MUSL)
    return &__musl_getaddrinfo;
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
            if (!DB::NameResolutionFilter::isAllowed(node, service ? std::string_view(service) : std::string_view()))
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
