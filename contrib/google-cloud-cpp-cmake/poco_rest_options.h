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
#include <string>

#include <Poco/Net/HTTPClientSession.h>

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

/// Called by the transport right before every HTTP request it sends, with the request method and
/// its path-and-query. It lets ClickHouse count the REST calls the storage library makes on its own
/// (the library pages `objects.list` lazily behind a `ListObjectsReader`, so the call site cannot
/// see how many requests a listing really costs). The observer runs on the thread issuing the
/// request and must not throw.
struct PocoRestRequestObserverOption
{
    using Type = std::function<void(const std::string & method, const std::string & path_and_query)>;
};

}
