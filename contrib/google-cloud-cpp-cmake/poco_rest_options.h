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

#include <Poco/Net/HTTPClientSession.h>

namespace ClickHouse
{

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

}
