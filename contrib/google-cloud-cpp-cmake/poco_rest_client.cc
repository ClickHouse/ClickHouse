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
#include <istream>
#include <memory>
#include <numeric>
#include <string>
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

std::unique_ptr<Poco::Net::HTTPClientSession> MakeSession(
    Poco::URI const& uri, Options const& options) {
  std::unique_ptr<Poco::Net::HTTPClientSession> session;
  if (uri.getScheme() == "https") {
    std::string ca_location;
    if (options.has<CARootsFilePathOption>()) {
      ca_location = options.get<CARootsFilePathOption>();
    }
    // When an explicit CA bundle is configured via CARootsFilePathOption, trust
    // only that bundle and do not also load the system trust store. This matches
    // the libcurl-based transport, where CAINFO replaces the default trust roots
    // rather than extending them, preserving custom-CA/pinning semantics.
    Poco::Net::Context::Ptr context(new Poco::Net::Context(
        Poco::Net::Context::TLSV1_2_CLIENT_USE, /*privateKeyFile=*/"",
        /*certificateFile=*/"", ca_location,
        Poco::Net::Context::VERIFY_STRICT, /*verificationDepth=*/9,
        /*loadDefaultCAs=*/ca_location.empty()));
    session = std::make_unique<Poco::Net::HTTPSClientSession>(
        uri.getHost(), uri.getPort(), context);
  } else {
    session = std::make_unique<Poco::Net::HTTPClientSession>(uri.getHost(),
                                                             uri.getPort());
  }
  if (options.has<ProxyOption>()) {
    auto const& proxy = options.get<ProxyOption>();
    if (!proxy.hostname().empty()) {
      // ProxyConfig defaults the scheme to "https"; default the port to match
      // the configured scheme instead of always assuming the plain-HTTP port,
      // matching the `scheme://host[:port]` proxy URL used by the libcurl-based
      // transport.
      Poco::UInt16 port = proxy.scheme() == "https"
                              ? Poco::Net::HTTPSClientSession::HTTPS_PORT
                              : Poco::Net::HTTPSession::HTTP_PORT;
      if (!proxy.port().empty()) {
        port = static_cast<Poco::UInt16>(std::stoi(proxy.port()));
      }
      session->setProxy(proxy.hostname(), port);
      if (!proxy.username().empty()) {
        session->setProxyCredentials(proxy.username(), proxy.password());
      }
    }
  }
  auto timeout = Poco::Timespan(RequestTimeout(options).count(), 0);
  session->setTimeout(/*connectionTimeout=*/Poco::Timespan(30, 0),
                      /*sendTimeout=*/timeout, /*receiveTimeout=*/timeout);
  return session;
}

// Streams the response body. Owns the session to keep the connection (and the
// std::istream obtained from it) alive until the payload is consumed.
class PocoHttpPayload : public HttpPayload {
 public:
  PocoHttpPayload(std::unique_ptr<Poco::Net::HTTPClientSession> session,
                  std::unique_ptr<Poco::Net::HTTPResponse> response,
                  std::istream* body)
      : session_(std::move(session)),
        response_(std::move(response)),
        body_(body) {}

  bool HasUnreadData() const override { return !finished_; }

  StatusOr<std::size_t> Read(absl::Span<char> buffer) override {
    if (finished_ || buffer.empty()) return std::size_t{0};
    try {
      body_->read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
      auto count = static_cast<std::size_t>(body_->gcount());
      if (body_->eof() || count == 0) finished_ = true;
      if (body_->bad()) {
        return Status(StatusCode::kUnavailable, "error reading response body");
      }
      return count;
    } catch (Poco::Exception const& e) {
      return Status(StatusCode::kUnavailable,
                    "error reading response body: " + e.displayText());
    }
  }

 private:
  std::unique_ptr<Poco::Net::HTTPClientSession> session_;
  std::unique_ptr<Poco::Net::HTTPResponse> response_;
  std::istream* body_;
  bool finished_ = false;
};

class PocoRestResponse : public RestResponse {
 public:
  PocoRestResponse(std::unique_ptr<Poco::Net::HTTPClientSession> session,
                   std::unique_ptr<Poco::Net::HTTPResponse> response,
                   std::istream* body) {
    status_code_ = static_cast<HttpStatusCode>(response->getStatus());
    for (auto const& header : *response) {
      auto name = header.first;
      std::transform(name.begin(), name.end(), name.begin(),
                     [](unsigned char c) { return std::tolower(c); });
      headers_.emplace(std::move(name), header.second);
    }
    payload_ = std::make_unique<PocoHttpPayload>(std::move(session),
                                                 std::move(response), body);
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
        url = Poco::URI(Poco::URI(url), location->second).toString();
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
    Poco::URI uri(url);
    auto session = MakeSession(uri, options);

    auto path = uri.getPathAndQuery();
    if (path.empty()) path = "/";
    Poco::Net::HTTPRequest http_request(
        method, path, Poco::Net::HTTPMessage::HTTP_1_1);
    if (options.has<AuthorityOption>()) {
      auto const& authority = options.get<AuthorityOption>();
      if (!authority.empty()) http_request.set("Host", authority);
    }
    http_request.set("User-Agent", UserAgent(options));
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
        std::move(session), std::move(http_response), &response_stream);
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

// Poco::Net::HTTPClientSession is not thread-safe and a streaming response
// keeps its session alive until the payload is fully consumed, so connections
// are not pooled here: each request creates a fresh session. ConnectionPoolSizeOption
// is therefore intentionally ignored. This trades some connection-reuse
// throughput for a simpler, correct transport; a Poco-backed pool can be added
// later if GCS request rates make it worthwhile.
std::unique_ptr<RestClient> MakePooledRestClient(std::string endpoint_address,
                                                 Options options) {
  return MakeDefaultRestClient(std::move(endpoint_address),
                               std::move(options));
}

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace rest_internal
}  // namespace cloud
}  // namespace google
