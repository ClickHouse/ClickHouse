// Options understood only by the Poco::Net based REST transport that ClickHouse substitutes for the
// libcurl-based one (see poco_rest_client.cc). They live in a ClickHouse-owned namespace, so they
// cannot collide with an upstream option, and in a header of their own, so both the transport and the
// ClickHouse code that builds a `google::cloud::Options` can refer to the same option type.
//
// `google::cloud::Options` accepts arbitrary option types; the storage library never validates the
// set of options against a predefined list, so an unknown-to-upstream option is simply carried
// through to the transport.

#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <string>

#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>

namespace ClickHouse
{

/// The Google "authorized user" refresh-token triple (the `google_adc_*` keys of the shared argument
/// grammar). google-cloud-cpp has no public credential factory for it: only its Application Default
/// Credentials path parses an `authorized_user` JSON document, and only from a file or the environment.
/// So the transport builds the SDK's own `oauth2_internal::AuthorizedUserCredentials` from the triple
/// instead, which is what makes the access token *refreshable* -- the refresh token is exchanged again
/// whenever the cached access token nears expiry, so a long-lived disk, or a query outliving the first
/// token, keeps working. When this option is set it supersedes `UnifiedCredentialsOption`.
struct PocoRestAuthorizedUserOption
{
    struct AuthorizedUser
    {
        std::string client_id;
        std::string client_secret;
        std::string refresh_token;
        /// Empty means the standard Google OAuth 2.0 refresh endpoint.
        std::string token_uri;
    };

    using Type = AuthorizedUser;
};

/// TCP connection timeout of a single REST request. The libcurl-based transport derives it from
/// CURLOPT_CONNECTTIMEOUT; the Poco transport has no equivalent upstream option, and its default
/// (30 seconds) is much larger than the ClickHouse default for object storage, so the value has to
/// be threaded in explicitly. Unset means "keep the transport default".
struct PocoRestConnectTimeoutOption
{
    using Type = std::chrono::milliseconds;
};

/// Provides the TLS context of the HTTPS sessions the transport opens. ClickHouse supplies the client
/// context configured in `openSSL.client` (`Poco::Net::SSLManager::defaultClientContext`: the CA
/// configuration, `loadDefaultCAFile`, the client certificate and key, the verification mode), which is
/// what the S3 transport uses too. Unset, or a provider returning null, means the transport builds its own
/// context from `CARootsFilePathOption`, or from the default trust store without it. The provider is called
/// for every request, so a context replaced at runtime applies to the sessions opened afterwards, and the
/// context is part of the session pool key (by identity): a pooled session is reused only by a request that
/// would have opened it with the same context. The provider runs on the thread issuing the request; it may
/// throw, and the request then fails without connecting.
struct PocoRestSslContextProviderOption
{
    using Type = std::function<Poco::Net::Context::Ptr()>;
};

/// Proxy of a single REST request, in the form the Poco session takes it. Upstream's `ProxyOption`
/// is a fixed value baked into the client, while ClickHouse resolves the proxy per request (the
/// proxy list rotates, and the remote resolver can hand out a different proxy over time), so the
/// transport asks for it once per session instead. The provider returns a default-constructed
/// `ProxyConfig` (empty `host`) to mean "no proxy for this request"; an unset option, or an empty
/// `std::function`, means the transport falls back to upstream's `ProxyOption`.
struct PocoRestProxyConfigProviderOption
{
    using Type = std::function<Poco::Net::HTTPClientSession::ProxyConfig()>;
};

/// Decides whether a pooled keep-alive session is stale and must not be handed out again. A socket
/// that is merely `connected()` is not necessarily reusable: the peer may already have queued an
/// unsolicited response (a `408 Request Timeout` sent before closing an idle connection), or sent a
/// TLS `close_notify` or a FIN, and the next request would then fail spuriously or parse leftover
/// bytes as its own response. The probe itself lives in ClickHouse (`DB::getSocketState`, which is
/// TLS-aware and tells real application data and a real close apart from a harmless TLS
/// post-handshake record) rather than here, so the transport does not have to depend on the
/// ClickHouse libraries. Unset means the pool falls back to `connected()` alone. The predicate runs
/// under the pool's lock, on the thread borrowing the session, and must not throw.
struct PocoRestSessionStaleCheckOption
{
    using Type = std::function<bool(Poco::Net::HTTPClientSession &)>;
};

/// Keep-alive policy of the pooled sessions, in the shape ClickHouse configures it everywhere else
/// (`http_keep_alive_timeout` / `http_keep_alive_max_requests` of the shared `s3`/disk grammar).
/// `Poco::Net::HTTPClientSession` enforces both itself -- it counts the requests it served and
/// reconnects once either bound is reached -- so the transport only has to apply them to every
/// session it opens, and to stop pooling a session that has already reached them. Poco does not
/// allow changing them on a connected session, so the transport keys its pool on them instead of
/// reapplying them to a pooled one. A zero value of either field means "keep the transport default
/// for it".
struct PocoRestKeepAliveOption
{
    struct KeepAlive
    {
        std::chrono::seconds timeout{0};
        std::size_t max_requests = 0;
    };

    using Type = KeepAlive;
};

/// Called with the proxy configuration a request was sent through, whenever that request failed with
/// a transport error or a server-side (5xx) error. `RemoteProxyConfigurationResolver` caches the
/// proxy it resolved for the whole TTL of its list, and this is how it is told to drop that cache and
/// ask the proxy resolver again, so a dead proxy is failed over from rather than retried until the
/// TTL expires. The S3 transport wires the same callback (`PocoHTTPClientConfiguration::error_report`).
/// The callback runs on the thread issuing the request and must not throw.
struct PocoRestProxyErrorReportOption
{
    using Type = std::function<void(const Poco::Net::HTTPClientSession::ProxyConfig &)>;
};

/// The proxy provider and error reporter of the token requests of the `PocoRestAuthorizedUserOption`
/// credential, used there instead of `PocoRestProxyConfigProviderOption` / `PocoRestProxyErrorReportOption`.
/// The proxy is resolved by the scheme of the request (`<proxy><http>` or `<proxy><https>`), and the
/// token endpoint can use a different scheme than the storage endpoint, so it may need a different
/// resolver. Unset means the token requests use the storage options.
struct PocoRestTokenProxyConfigProviderOption
{
    using Type = std::function<Poco::Net::HTTPClientSession::ProxyConfig()>;
};

struct PocoRestTokenProxyErrorReportOption
{
    using Type = std::function<void(const Poco::Net::HTTPClientSession::ProxyConfig &)>;
};

/// Called by the transport with the method of every storage request, before it borrows a session for
/// it, so that ClickHouse can apply its request-rate throttlers (`s3_max_get_rps` / `s3_max_put_rps`
/// and their bursts, the same limits the S3 transport applies in `PocoHTTPClient`). It may block for
/// as long as the throttler requires. It runs on the thread issuing the request, is called for every
/// attempt of a retried request, and must not throw. It is not called for the token requests of the
/// `PocoRestAuthorizedUserOption` credential, which go to a different service.
struct PocoRestRequestThrottleOption
{
    using Type = std::function<void(const std::string & method)>;
};

/// Called by the transport right before every HTTP request it sends, with the request method and
/// its path-and-query. It lets ClickHouse count the REST calls the storage library makes on its own
/// (the library pages `objects.list` lazily behind a `ListObjectsReader`, so the call site cannot
/// see how many requests a listing really costs). The observer runs on the thread issuing the
/// request and must not throw.
struct PocoRestRequestObserverOption
{
    using Type = std::function<void(const std::string & method, const std::string & path_and_query)>;
};

/// Called by the transport with the full URL of every request before it borrows a session for it,
/// including a URL the server handed out (the session URL of a resumable upload is the `Location` of
/// the response that started it) and a redirect target. It returns an empty string when the URL may be
/// requested, otherwise the reason it may not, and the request then fails without connecting. ClickHouse
/// checks the URL against `remote_url_allow_hosts` here. It runs on the thread issuing the request and
/// must not throw.
struct PocoRestUrlCheckOption
{
    using Type = std::function<std::string(const std::string & url)>;
};

/// Accounts the sessions the transport opens in a ClickHouse connection group (the `DISK` group for a
/// disk, the `STORAGE` group otherwise), as `HTTPConnectionPools` accounts its own connections, so that
/// switching to the native transport does not escape `disk_connections_hard_limit` /
/// `storage_connections_hard_limit`, the group's limit on idle connections, and the group's
/// `rcvbuf` / `sndbuf`. The factory is called for every session the transport creates, before it
/// connects, and throws when the group's hard limit is reached; the request then fails without
/// connecting. The returned registration lives exactly as long as the session and is destroyed right
/// before it. Unset means the sessions are not accounted.
///
/// A registration stays with its session while the session is pooled, so the transport hands a pooled
/// session only to a request accounted in the same `group` (or, for an unaccounted session, to an
/// unaccounted request): otherwise a request could borrow a session counted in another group and skip
/// the limits of its own.
struct PocoRestSessionAccountingOption
{
    class Registration
    {
    public:
        /// Unregisters the session.
        virtual ~Registration() = default;

        /// Called after every request was sent on the session, which is then connected (it may have
        /// reconnected since the previous request). May throw; the request then fails.
        virtual void onConnected(Poco::Net::HTTPClientSession & session) = 0;

        /// Called before the session is kept idle for reuse. Returns false when the session must not be
        /// kept, and may throw (the session is then not kept either).
        virtual bool tryReserveIdle() = 0;

        /// Called exactly once after a successful `tryReserveIdle`, when the session stops being idle
        /// (it is handed out again, or dropped from the pool).
        virtual void releaseIdle() noexcept = 0;
    };

    using Factory = std::function<std::unique_ptr<Registration>(Poco::Net::HTTPClientSession & session)>;

    struct Type
    {
        /// Identifies the connection group the factory registers sessions in; part of the session pool key.
        std::string group;
        Factory factory;
    };
};

/// Called by the transport on the session of every request right before the request is sent, so that
/// ClickHouse can install the IO scheduling hooks of the current thread on it (`ResourceGuard` for the
/// read and write resources, as `HTTPConnectionPool` does), or remove the hooks of a previous request
/// when the current thread has none. It runs on the thread issuing the request.
struct PocoRestSessionHooksOption
{
    using Type = std::function<void(Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request)>;
};

/// Closes every idle keep-alive session the transport retains for reuse, process-wide, and gives back the idle
/// slots they hold in their connection groups. Sessions in use are not affected. `SYSTEM DROP CONNECTIONS CACHE`
/// calls it next to `HTTPConnectionPools::dropCache`, as the transport keeps its sessions in a pool of its own.
void dropPocoRestIdleSessions();

}
