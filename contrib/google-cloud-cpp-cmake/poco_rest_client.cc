// A Poco::Net based implementation of google::cloud::rest_internal::RestClient.
//
// ClickHouse builds google-cloud-cpp without libcurl. This file replaces the
// libcurl-based transport (curl_impl.cc, curl_rest_client.cc, ...) excluded in
// cmake/google_cloud_cpp_rest_internal.cmake: it defines `MakeDefaultRestClient`,
// `MakePooledRestClient` and a no-op `CurlInitializeOnce`.

#include "google/cloud/common_options.h"
#include "google/cloud/credentials.h"
#include "google/cloud/internal/curl_options.h"
#include "google/cloud/internal/http_payload.h"
#include "google/cloud/internal/oauth2_credentials.h"
#include "google/cloud/internal/rest_client.h"
#include "google/cloud/internal/rest_context.h"
#include "google/cloud/internal/rest_options.h"
#include "google/cloud/internal/rest_request.h"
#include "google/cloud/internal/rest_response.h"
#include "google/cloud/internal/unified_rest_credentials.h"
#include "google/cloud/internal/url_encode.h"
#include "google/cloud/options.h"
#include "google/cloud/version.h"

#include "poco_rest_options.h"

#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/NetException.h>
#include <Poco/Timespan.h>
#include <Poco/URI.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <functional>
#include <istream>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace rest_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN

// The libcurl SSL locking setup is not needed with Poco::Net.
void CurlInitializeOnce(Options const&) {}

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace rest_internal
}  // namespace cloud
}  // namespace google

namespace google {
namespace cloud {
namespace rest_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

auto constexpr kDefaultTimeout = std::chrono::seconds(120);
auto constexpr kDefaultConnectTimeout = std::chrono::seconds(30);
auto constexpr kMaxRedirects = 10;

// Strict RFC 3986 percent-encoding, matching curl_easy_escape: everything
// except unreserved characters is escaped. `internal::UrlEncode` is not
// suitable here as it leaves some reserved characters (e.g. "!*'()") as is.
std::string PercentEncode(std::string const& value) {
  auto constexpr kDigits = "0123456789ABCDEF";
  std::string result;
  result.reserve(value.size());
  for (unsigned char c : value) {
    if (std::isalnum(c) || c == '-' || c == '.' || c == '_' || c == '~') {
      result.push_back(static_cast<char>(c));
    } else {
      result.push_back('%');
      result.push_back(kDigits[(c >> 4) & 0xF]);
      result.push_back(kDigits[c & 0xF]);
    }
  }
  return result;
}

std::string ComposeUrl(std::string const& endpoint, RestRequest const& request,
                       Options const& options) {
  std::string url;
  auto const& path = request.path();
  if (path.rfind("http://", 0) == 0 || path.rfind("https://", 0) == 0) {
    url = path;
  } else {
    url = endpoint;
    while (!url.empty() && url.back() == '/') url.pop_back();
    if (path.empty() || path.front() != '/') url.push_back('/');
    url.append(path);
  }
  char separator = url.find('?') == std::string::npos ? '?' : '&';
  for (auto const& p : request.parameters()) {
    url.push_back(separator);
    url.append(PercentEncode(p.first));
    url.push_back('=');
    url.append(PercentEncode(p.second));
    separator = '&';
  }
  if (options.has<UserIpOption>()) {
    auto const& v = options.get<UserIpOption>();
    url.push_back(separator);
    url.append(v.empty() ? "userIp=" : "userIp=" + PercentEncode(v));
  }
  return url;
}

std::string UserAgent(Options const& options) {
  std::string result;
  if (options.has<UserAgentProductsOption>()) {
    for (auto const& p : options.get<UserAgentProductsOption>()) {
      result += p;
      result += ' ';
    }
  }
  result += "gcloud-cpp/" + version_string() + " (Poco)";
  return result;
}

std::chrono::seconds RequestTimeout(Options const& options) {
  auto timeout = kDefaultTimeout;
  if (options.has<TransferStallTimeoutOption>()) {
    auto v = options.get<TransferStallTimeoutOption>();
    if (v.count() != 0) timeout = v;
  }
  if (options.has<DownloadStallTimeoutOption>()) {
    auto v = options.get<DownloadStallTimeoutOption>();
    if (v.count() != 0) timeout = (std::max)(timeout, v);
  }
  return timeout;
}

// Building a Poco::Net::Context creates an OpenSSL SSL_CTX: it loads the trust
// store (a `getdents64`/`stat`/`open` walk of the CA directory) and builds the
// cipher list. That is per-*context* work, not per-connection, and the context is
// immutable once configured and safe to share, so cache one per CA location
// instead of rebuilding it for every request.
Poco::Net::Context::Ptr SslContextFor(std::string const& ca_location) {
  static std::mutex mu;
  static auto* contexts =
      new std::unordered_map<std::string, Poco::Net::Context::Ptr>();
  std::lock_guard<std::mutex> lock(mu);
  auto it = contexts->find(ca_location);
  if (it != contexts->end()) return it->second;
  // When an explicit CA bundle is configured via CARootsFilePathOption, trust
  // only that bundle and do not also load the system trust store. This matches
  // the libcurl-based transport, where CAINFO replaces the default trust roots
  // rather than extending them, preserving custom-CA/pinning semantics.
  Poco::Net::Context::Ptr context(new Poco::Net::Context(
      Poco::Net::Context::TLSV1_2_CLIENT_USE, /*privateKeyFile=*/"",
      /*certificateFile=*/"", ca_location,
      Poco::Net::Context::VERIFY_STRICT, /*verificationDepth=*/9,
      /*loadDefaultCAs=*/ca_location.empty()));
  contexts->emplace(ca_location, context);
  return context;
}

// Idle keep-alive sessions, so a request does not pay a DNS lookup, a TCP
// handshake and a TLS handshake of its own. `Poco::Net::HTTPClientSession` is not
// thread-safe, so the pool hands out exclusive ownership and takes it back only
// once the response body has been fully consumed -- a session with unread bytes
// still buffered would desynchronise the next request on that socket.
class SessionPool {
 public:
  static SessionPool& Instance() {
    static auto* pool = new SessionPool();
    return *pool;
  }

  std::unique_ptr<Poco::Net::HTTPClientSession> Acquire(std::string const& key) {
    auto const now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mu_);
    auto it = idle_.find(key);
    if (it == idle_.end()) return nullptr;
    auto& entries = it->second;
    while (!entries.empty()) {
      auto entry = std::move(entries.back());
      entries.pop_back();
      // A peer may close an idle keep-alive connection at any time. `connected()`
      // does not detect a half-closed socket, so also drop anything that has sat
      // idle long enough to be a likely candidate, and let the caller connect
      // afresh rather than discover the failure mid-request.
      if (now - entry.returned_at < kMaxIdleTime && entry.session->connected()) {
        return std::move(entry.session);
      }
    }
    idle_.erase(it);
    return nullptr;
  }

  void Release(std::string const& key,
               std::unique_ptr<Poco::Net::HTTPClientSession> session) {
    if (!session || !session->connected()) return;
    std::lock_guard<std::mutex> lock(mu_);
    auto& entries = idle_[key];
    if (entries.size() >= max_per_endpoint_) return;
    entries.push_back({std::move(session), std::chrono::steady_clock::now()});
  }

  void SetMaxPerEndpoint(std::size_t value) {
    std::lock_guard<std::mutex> lock(mu_);
    if (value > 0) max_per_endpoint_ = value;
  }

 private:
  struct Entry {
    std::unique_ptr<Poco::Net::HTTPClientSession> session;
    std::chrono::steady_clock::time_point returned_at;
  };

  static constexpr auto kMaxIdleTime = std::chrono::seconds(20);

  std::mutex mu_;
  std::unordered_map<std::string, std::vector<Entry>> idle_;
  std::size_t max_per_endpoint_ = 64;
};

// Identifies connections that are interchangeable: same transport endpoint and
// same proxy. Anything that changes the socket's peer, or the way the request
// travels to it, has to be part of the key. The proxy *mode* matters as much as
// its address: a tunnelled (CONNECT) socket carries the request to the origin
// server while a non-tunnelled one carries it to the proxy itself, and the
// proxy protocol, credentials and bypass pattern decide how the connection is
// established. Handing a session opened under one mode to a request configured
// for another would silently apply the first mode (the proxy configuration of a
// pooled session is not reapplied). ClickHouse's own `HTTPConnectionPool` keys
// on the same discriminators.
//
// The proxy password is part of the credentials Poco keeps in the session and
// replays for `Proxy-Authorization` and `CONNECT`, so it discriminates too:
// without it, rotating only the secret would keep reusing a socket that was
// authenticated with the previous one. It enters the key as a hash rather than
// verbatim, so the pool does not hold a second plaintext copy of the secret for
// the lifetime of the session.
std::string SessionKey(
    Poco::URI const& uri,
    Poco::Net::HTTPClientSession::ProxyConfig const& proxy) {
  return uri.getScheme() + "://" + uri.getHost() + ":" +
         std::to_string(uri.getPort()) + "|" + proxy.host + ":" +
         std::to_string(proxy.port) + "|" + proxy.protocol + "|" +
         (proxy.tunnel ? "tunnel" : "direct") + "|" +
         proxy.originalRequestProtocol + "|" + proxy.username + "|" +
         std::to_string(std::hash<std::string>{}(proxy.password)) + "|" +
         proxy.nonProxyHosts;
}

// Returns a connected-or-connectable session for `uri`, reusing a pooled one when
// the endpoint and proxy match. `session_key` is set to the pool key so the
// caller can hand the session back once the response body has been consumed.
std::unique_ptr<Poco::Net::HTTPClientSession> MakeSession(
    Poco::URI const& uri, Options const& options, std::string* session_key) {
  if (options.has<ConnectionPoolSizeOption>()) {
    SessionPool::Instance().SetMaxPerEndpoint(
        options.get<ConnectionPoolSizeOption>());
  }

  // The proxy has to be resolved before the pool is consulted: it decides which
  // peer the socket is actually connected to, so it is part of the pool key. The
  // provider is called exactly once per request, as before.
  bool proxy_from_provider = false;
  Poco::Net::HTTPClientSession::ProxyConfig provider_proxy;
  if (options.has<::ClickHouse::PocoRestProxyConfigProviderOption>()) {
    auto const& provider =
        options.get<::ClickHouse::PocoRestProxyConfigProviderOption>();
    if (provider) {
      proxy_from_provider = true;
      provider_proxy = provider();
    }
  }
  // The proxy configuration the session ends up with, in the exact shape Poco
  // applies it. It is both what a freshly created session is configured with
  // below and what the pool key is derived from, so a pooled session is only
  // ever reused by a request whose proxy behaves the same way.
  Poco::Net::HTTPClientSession::ProxyConfig effective_proxy;
  if (proxy_from_provider) {
    effective_proxy = provider_proxy;
  } else if (options.has<ProxyOption>()) {
    auto const& proxy = options.get<ProxyOption>();
    if (!proxy.hostname().empty()) {
      effective_proxy.host = proxy.hostname();
      // ProxyConfig defaults the scheme to "https"; default the port to match
      // the configured scheme instead of always assuming the plain-HTTP port,
      // matching the `scheme://host[:port]` proxy URL used by the libcurl-based
      // transport.
      effective_proxy.port = proxy.scheme() == "https"
                                 ? Poco::Net::HTTPSClientSession::HTTPS_PORT
                                 : Poco::Net::HTTPSession::HTTP_PORT;
      if (!proxy.port().empty()) {
        effective_proxy.port = static_cast<Poco::UInt16>(std::stoi(proxy.port()));
      }
      effective_proxy.username = proxy.username();
      effective_proxy.password = proxy.password();
    }
  }
  if (effective_proxy.host.empty()) effective_proxy = {};

  auto const key = SessionKey(uri, effective_proxy);
  if (session_key != nullptr) *session_key = key;

  // A pooled session already points at this endpoint through this proxy -- both
  // are in the key -- so only a fresh one needs constructing and configuring.
  auto session = SessionPool::Instance().Acquire(key);
  if (!session) {
    if (uri.getScheme() == "https") {
      std::string ca_location;
      if (options.has<CARootsFilePathOption>()) {
        ca_location = options.get<CARootsFilePathOption>();
      }
      session = std::make_unique<Poco::Net::HTTPSClientSession>(
          uri.getHost(), uri.getPort(), SslContextFor(ca_location));
    } else {
      session = std::make_unique<Poco::Net::HTTPClientSession>(uri.getHost(),
                                                               uri.getPort());
    }
    // A per-request proxy resolved by ClickHouse wins over the fixed upstream
    // ProxyOption: it already carries everything Poco needs (including tunneling
    // and the no-proxy host pattern), and it can change between two requests of
    // the same client. Both were resolved above, before the pool lookup.
    if (!effective_proxy.host.empty()) {
      if (proxy_from_provider) {
        session->setProxyConfig(effective_proxy);
      } else {
        session->setProxy(effective_proxy.host, effective_proxy.port);
        if (!effective_proxy.username.empty()) {
          session->setProxyCredentials(effective_proxy.username,
                                       effective_proxy.password);
        }
      }
    }
  }

  // Applied to pooled sessions too: the timeouts come from the options of the
  // client making *this* request, which need not be the one that opened the
  // connection.
  auto timeout = Poco::Timespan(RequestTimeout(options).count(), 0);
  auto connection_timeout = Poco::Timespan(kDefaultConnectTimeout.count(), 0);
  if (options.has<::ClickHouse::PocoRestConnectTimeoutOption>()) {
    auto const v = options.get<::ClickHouse::PocoRestConnectTimeoutOption>();
    if (v.count() > 0) {
      connection_timeout = Poco::Timespan(
          static_cast<Poco::Timespan::TimeDiff>(v.count()) * 1000);
    }
  }
  session->setTimeout(connection_timeout,
                      /*sendTimeout=*/timeout, /*receiveTimeout=*/timeout);
  // Without this Poco sends `Connection: Close` and the peer tears the socket
  // down after one response, which would make the pool useless.
  session->setKeepAlive(true);
  return session;
}

// Streams the response body. Owns the session to keep the connection (and the
// std::istream obtained from it) alive until the payload is consumed.
class PocoHttpPayload : public HttpPayload {
 public:
  PocoHttpPayload(std::unique_ptr<Poco::Net::HTTPClientSession> session,
                  std::unique_ptr<Poco::Net::HTTPResponse> response,
                  std::istream* body, std::string session_key)
      : session_(std::move(session)),
        response_(std::move(response)),
        body_(body),
        session_key_(std::move(session_key)) {}

  // The session goes back to the pool only when this response was read to the
  // end and both sides agreed to keep the connection: a socket with unread body
  // bytes still buffered would desynchronise whichever request picked it up next.
  // Whether that holds is not always known by the time the body is dropped --
  // see FinishForReuse.
  ~PocoHttpPayload() override {
    if (!session_) return;
    if (!failed_ && !finished_) FinishForReuse();
    if (!finished_ || failed_) return;
    if (!response_ || !response_->getKeepAlive()) return;
    SessionPool::Instance().Release(session_key_, std::move(session_));
  }

  bool HasUnreadData() const override { return !finished_; }

  StatusOr<std::size_t> Read(absl::Span<char> buffer) override {
    if (finished_ || buffer.empty()) return std::size_t{0};
    try {
      body_->read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
      auto count = static_cast<std::size_t>(body_->gcount());
      bytes_read_ += count;
      if (body_->eof() || count == 0) finished_ = true;
      if (body_->bad()) {
        failed_ = true;
        return Status(StatusCode::kUnavailable, "error reading response body");
      }
      return count;
    } catch (Poco::Exception const& e) {
      failed_ = true;
      return Status(StatusCode::kUnavailable,
                    "error reading response body: " + e.displayText());
    }
  }

 private:
  // Decide whether this session can still be pooled, for the two recoverable cases where the body
  // was dropped without `Read` having marked the payload finished. Both need a known
  // `Content-Length`: a chunked or unknown-length response cannot be reasoned about and is left
  // alone, and knowing the length up front means the cost below is known rather than discovered by
  // blocking on the network.
  //
  // The first case is a body consumed exactly to its end. `Read` only sets `finished_` once the
  // stream reports `eof`, which takes one read past the last byte, and a bounded reader never
  // issues it: `ReadBufferFromGCS::nextImpl` returns as soon as it holds the bytes up to
  // `read_until_position`. Nothing is left unread on the socket, so the session is reusable as is.
  // This is the ordinary shape of a `MergeTree` read, and it is what decides whether connections
  // get reused at all -- treating it as unreusable closes a connection per ranged read.
  //
  // The second is a read the caller abandoned partway because it seeked elsewhere. Reading the
  // tail out makes the session reusable, and while the tail is short that costs less than the DNS
  // lookup, TCP connect and TLS handshake a replacement connection would pay. The budget keeps
  // that trade honest: a long tail is not worth transferring, so the socket is closed instead.
  //
  // Anything unexpected leaves `finished_` false, which means "do not reuse" -- the conservative
  // direction.
  void FinishForReuse() {
    // Roughly the transfer time of a TLS handshake's round trip on an intra-region link. Above
    // this, reconnecting is the cheaper of the two.
    static constexpr std::uint64_t kMaxDrainBytes = 128 * 1024;

    if (!response_) return;
    auto const content_length = response_->getContentLength();
    if (content_length == Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH) return;
    auto const total = static_cast<std::uint64_t>(content_length);

    if (bytes_read_ >= total) {
      finished_ = true;
      return;
    }

    auto remaining = total - bytes_read_;
    if (remaining > kMaxDrainBytes) return;

    try {
      char scratch[16 * 1024];
      while (remaining > 0) {
        auto const want = static_cast<std::streamsize>(
            std::min<std::uint64_t>(remaining, sizeof(scratch)));
        body_->read(scratch, want);
        auto const count = static_cast<std::uint64_t>(body_->gcount());
        if (count == 0 || body_->bad()) return;
        remaining -= count;
      }
      finished_ = true;
    } catch (...) {
      // Destructor: swallow and leave the session unreusable.
    }
  }

  std::unique_ptr<Poco::Net::HTTPClientSession> session_;
  std::unique_ptr<Poco::Net::HTTPResponse> response_;
  std::istream* body_;
  std::string session_key_;
  std::uint64_t bytes_read_ = 0;
  bool finished_ = false;
  bool failed_ = false;
};

class PocoRestResponse : public RestResponse {
 public:
  PocoRestResponse(std::unique_ptr<Poco::Net::HTTPClientSession> session,
                   std::unique_ptr<Poco::Net::HTTPResponse> response,
                   std::istream* body, std::string session_key) {
    status_code_ = static_cast<HttpStatusCode>(response->getStatus());
    for (auto const& header : *response) {
      auto name = header.first;
      std::transform(name.begin(), name.end(), name.begin(),
                     [](unsigned char c) { return std::tolower(c); });
      headers_.emplace(std::move(name), header.second);
    }
    payload_ = std::make_unique<PocoHttpPayload>(
        std::move(session), std::move(response), body, std::move(session_key));
  }

  HttpStatusCode StatusCode() const override { return status_code_; }

  std::multimap<std::string, std::string> Headers() const override {
    return headers_;
  }

  std::unique_ptr<HttpPayload> ExtractPayload() && override {
    return std::move(payload_);
  }

 private:
  HttpStatusCode status_code_;
  std::multimap<std::string, std::string> headers_;
  std::unique_ptr<HttpPayload> payload_;
};

class PocoRestClient : public RestClient {
 public:
  PocoRestClient(std::string endpoint, Options options)
      : endpoint_(std::move(endpoint)), options_(std::move(options)) {
    // Map the unified credentials to an OAuth 2.0 credential so an
    // `Authorization` header can be added to every request, matching the
    // libcurl-based transport. This is required for token-exchange and
    // impersonation flows (external-account and impersonated service-account
    // credentials build their token-fetching REST clients via
    // `MakeDefaultRestClient`), which would otherwise be sent unauthenticated.
    if (options_.has<UnifiedCredentialsOption>()) {
      credentials_ = MapCredentials(*options_.get<UnifiedCredentialsOption>());
    }
  }

  StatusOr<std::unique_ptr<RestResponse>> Delete(
      RestContext& context, RestRequest const& request) override {
    return MakeRequest(context, request, Poco::Net::HTTPRequest::HTTP_DELETE,
                       {});
  }

  StatusOr<std::unique_ptr<RestResponse>> Get(
      RestContext& context, RestRequest const& request) override {
    return MakeRequest(context, request, Poco::Net::HTTPRequest::HTTP_GET, {});
  }

  StatusOr<std::unique_ptr<RestResponse>> Patch(
      RestContext& context, RestRequest const& request,
      std::vector<absl::Span<char const>> const& payload) override {
    return MakeRequest(context, request, Poco::Net::HTTPRequest::HTTP_PATCH,
                       payload);
  }

  StatusOr<std::unique_ptr<RestResponse>> Post(
      RestContext& context, RestRequest const& request,
      std::vector<absl::Span<char const>> const& payload) override {
    return MakeRequest(context, request, Poco::Net::HTTPRequest::HTTP_POST,
                       payload);
  }

  StatusOr<std::unique_ptr<RestResponse>> Post(
      RestContext& context, RestRequest const& request,
      std::vector<std::pair<std::string, std::string>> const& form_data)
      override {
    std::string body;
    char separator = 0;
    for (auto const& p : form_data) {
      if (separator != 0) body.push_back(separator);
      body.append(PercentEncode(p.first));
      body.push_back('=');
      body.append(PercentEncode(p.second));
      separator = '&';
    }
    // The body is already percent-encoded here, so mark its Content-Type
    // explicitly to stop MakeRequest() from re-encoding it as a default form
    // body (matching the libcurl-based transport, which set this header on the
    // context for the form_data overload only).
    context.AddHeader("content-type", "application/x-www-form-urlencoded");
    return MakeRequest(context, request, Poco::Net::HTTPRequest::HTTP_POST,
                       {{body.data(), body.size()}});
  }

  StatusOr<std::unique_ptr<RestResponse>> Put(
      RestContext& context, RestRequest const& request,
      std::vector<absl::Span<char const>> const& payload) override {
    return MakeRequest(context, request, Poco::Net::HTTPRequest::HTTP_PUT,
                       payload);
  }

 private:
  StatusOr<std::unique_ptr<RestResponse>> MakeRequest(
      RestContext& context, RestRequest const& request,
      std::string const& method,
      std::vector<absl::Span<char const>> const& payload) const {
    // Per-call options (e.g. an OptionsSpan forwarded through RestContext) take
    // precedence over the client options. The libcurl-based transport merged
    // context.options() into the client options for every verb, so options like
    // the stall timeout, CA bundle, or proxy carried by an individual request
    // are honored throughout building the URL, session and headers.
    auto options = internal::MergeOptions(context.options(), options_);

    // Resolve the authentication header once per request, propagating a refresh
    // failure as a Status instead of sending an unauthenticated request.
    std::pair<std::string, std::string> auth_header;
    if (credentials_) {
      auto header =
          credentials_->AuthenticationHeader(std::chrono::system_clock::now());
      if (!header) return std::move(header).status();
      auth_header = *std::move(header);
    }

    // The libcurl-based transport only followed redirects when
    // CurlFollowLocationOption was enabled. It defaults to false and is not set
    // anywhere in ClickHouse, so by default the 3xx response is returned as-is.
    bool const follow_location = options.has<CurlFollowLocationOption>() &&
                                 options.get<CurlFollowLocationOption>();

    auto url = ComposeUrl(endpoint_, request, options);
    try {
      for (int redirect = 0; redirect != kMaxRedirects; ++redirect) {
        auto response = MakeSingleRequest(context, request, method, payload,
                                          url, options, auth_header);
        auto const status_code =
            static_cast<std::int32_t>(response->StatusCode());
        if (!follow_location ||
            status_code < HttpStatusCode::kMinRedirects ||
            status_code >= HttpStatusCode::kMinRequestErrors ||
            status_code == HttpStatusCode::kNotModified ||
            status_code == HttpStatusCode::kResumeIncomplete) {
          return std::unique_ptr<RestResponse>(std::move(response));
        }
        auto headers = response->Headers();
        auto location = headers.find("location");
        if (location == headers.end()) {
          return std::unique_ptr<RestResponse>(std::move(response));
        }
        // Resolve relative Location values against the current URL, as libcurl
        // does, so e.g. "Location: /new-path" keeps the current scheme and host.
        // Both sides are parsed with URL encoding disabled so that the merge
        // preserves every escape sequence verbatim (see `MakeSingleRequest`);
        // `Poco::URI(base, relative)` would instead re-parse the relative
        // reference with decoding enabled.
        Poco::URI resolved(url, /*enable_url_encoding=*/false);
        resolved.resolve(Poco::URI(location->second, /*enable_url_encoding=*/false));
        url = resolved.toString();
      }
      return Status(StatusCode::kUnavailable,
                    "too many redirects requesting " + url);
    } catch (Poco::TimeoutException const& e) {
      return Status(StatusCode::kDeadlineExceeded,
                    "request to " + url + " timed out: " + e.displayText());
    } catch (Poco::Exception const& e) {
      return Status(StatusCode::kUnavailable,
                    "request to " + url + " failed: " + e.displayText());
    }
  }

  std::unique_ptr<RestResponse> MakeSingleRequest(
      RestContext& context, RestRequest const& request,
      std::string const& method,
      std::vector<absl::Span<char const>> const& payload,
      std::string const& url, Options const& options,
      std::pair<std::string, std::string> const& auth_header) const {
    // The URL is already fully percent-encoded by the storage layer, so parse it
    // with ClickHouse's `enable_url_encoding = false` extension: by default
    // `Poco::URI` percent-*decodes* the path when it parses a URL and re-encodes
    // it with a reserved set that does not contain '/', so the round trip turns a
    // "%2F" back into a path separator. GCS object names routinely contain
    // slashes and are addressed in the request path
    // (".../o/mergetree%2Frsp%2Fabc"), while an upload carries the name in the
    // query string instead: the round trip therefore silently reads, heads and
    // deletes a *different*, non-existent resource while writes keep working.
    Poco::URI uri(url, /*enable_url_encoding=*/false);
    std::string session_key;
    auto session = MakeSession(uri, options, &session_key);

    auto path = uri.getPathAndQuery();
    if (path.empty()) path = "/";
    Poco::Net::HTTPRequest http_request(
        method, path, Poco::Net::HTTPMessage::HTTP_1_1);
    if (options.has<AuthorityOption>()) {
      auto const& authority = options.get<AuthorityOption>();
      if (!authority.empty()) http_request.set("Host", authority);
    }
    http_request.set("User-Agent", UserAgent(options));
    // Headers configured for every request of this client. They are added before
    // the authorization header and before the per-request headers, so neither can
    // be shadowed by a custom header.
    if (options.has<CustomHeadersOption>()) {
      for (auto const& header : options.get<CustomHeadersOption>()) {
        if (!header.second.empty()) http_request.add(header.first, header.second);
      }
    }
    if (!auth_header.first.empty()) {
      http_request.set(auth_header.first, auth_header.second);
    }
    // An empty value means "do not send this header at all". This matches the
    // semantics of the libcurl-based transport, where an empty value unsets a
    // header; e.g. the storage stub disables chunked transfer encoding by
    // adding `Transfer-Encoding` with an empty value.
    for (auto const& header : context.headers()) {
      for (auto const& value : header.second) {
        if (!value.empty()) http_request.add(header.first, value);
      }
    }
    for (auto const& header : request.headers()) {
      for (auto const& value : header.second) {
        if (!value.empty()) http_request.add(header.first, value);
      }
    }

    auto const payload_size = std::accumulate(
        payload.begin(), payload.end(), std::size_t{0},
        [](std::size_t n, auto const& s) { return n + s.size(); });
    auto const has_body = method == Poco::Net::HTTPRequest::HTTP_POST ||
                          method == Poco::Net::HTTPRequest::HTTP_PUT ||
                          method == Poco::Net::HTTPRequest::HTTP_PATCH;
    // Match the libcurl-based transport: a payload without an explicit
    // Content-Type defaults to application/x-www-form-urlencoded and the whole
    // body is percent-encoded before being sent. Requests that set their own
    // Content-Type (e.g. JSON metadata, media uploads, and the form_data Post
    // overload, whose body is already encoded) send the payload unchanged.
    std::string encoded_body;
    bool encode_form = false;
    if (has_body) {
      if (!http_request.has("Content-Type")) {
        http_request.set("Content-Type", "application/x-www-form-urlencoded");
        encode_form = true;
      }
      if (encode_form) {
        std::string concatenated;
        concatenated.reserve(payload_size);
        for (auto const& span : payload) {
          concatenated.append(span.data(), span.size());
        }
        encoded_body = PercentEncode(concatenated);
        http_request.setContentLength(
            static_cast<std::streamsize>(encoded_body.size()));
      } else {
        http_request.setContentLength(
            static_cast<std::streamsize>(payload_size));
      }
    }

    if (options.has<::ClickHouse::PocoRestRequestObserverOption>()) {
      auto const& observer =
          options.get<::ClickHouse::PocoRestRequestObserverOption>();
      if (observer) observer(method, path);
    }

    auto& body_stream = session->sendRequest(http_request);
    if (encode_form) {
      body_stream.write(encoded_body.data(),
                        static_cast<std::streamsize>(encoded_body.size()));
    } else {
      for (auto const& span : payload) {
        body_stream.write(span.data(),
                          static_cast<std::streamsize>(span.size()));
      }
    }

    auto http_response = std::make_unique<Poco::Net::HTTPResponse>();
    auto& response_stream = session->receiveResponse(*http_response);

    try {
      auto const& socket = session->socket();
      context.set_primary_ip_address(socket.peerAddress().host().toString());
      context.set_primary_port(socket.peerAddress().port());
      context.set_local_ip_address(socket.address().host().toString());
      context.set_local_port(socket.address().port());
    } catch (Poco::Exception const&) {
      // The metadata is optional, e.g. the socket may be closed already.
    }

    return std::make_unique<PocoRestResponse>(
        std::move(session), std::move(http_response), &response_stream,
        std::move(session_key));
  }

  std::string endpoint_;
  Options options_;
  std::shared_ptr<oauth2_internal::Credentials> credentials_;
};

}  // namespace

// Same as the implementation removed with curl_http_payload.cc.
StatusOr<std::string> ReadAll(std::unique_ptr<HttpPayload> payload,
                              std::size_t read_size) {
  std::string output_buffer;
  auto buf = std::make_unique<char[]>(read_size);
  StatusOr<std::size_t> read_status;
  do {
    read_status = payload->Read({&buf[0], read_size});
    if (!read_status.ok()) return std::move(read_status).status();
    output_buffer.append(buf.get(), read_status.value());
  } while (read_status.value() > 0);
  return output_buffer;
}

std::unique_ptr<RestClient> MakeDefaultRestClient(std::string endpoint_address,
                                                  Options options) {
  return std::make_unique<PocoRestClient>(std::move(endpoint_address),
                                          std::move(options));
}

// Both entry points share the same client: `MakeSession` already reuses idle
// keep-alive sessions through `SessionPool`, sized by ConnectionPoolSizeOption.
std::unique_ptr<RestClient> MakePooledRestClient(std::string endpoint_address,
                                                 Options options) {
  return MakeDefaultRestClient(std::move(endpoint_address),
                               std::move(options));
}

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace rest_internal
}  // namespace cloud
}  // namespace google
