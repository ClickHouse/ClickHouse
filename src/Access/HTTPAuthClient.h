#pragma once

#include <set>

#include <base/sleep.h>
#include <Common/Exception.h>
#include <Common/HTTPFieldLess.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/WriteHelpers.h>

#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/OAuth20Credentials.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct HTTPAuthClientParams
{
    Poco::URI uri;
    ConnectionTimeouts timeouts;
    size_t max_tries{};
    size_t retry_initial_backoff_ms{};
    size_t retry_max_backoff_ms{};
    std::vector<String> forward_headers;

    static HTTPAuthClientParams createDefault(const String & uri_, size_t max_tries_);
};

template <typename TResponseParser>
class HTTPAuthClientBase
{
public:
    using Result = TResponseParser::Result;

    explicit HTTPAuthClientBase(const HTTPAuthClientParams & params, const TResponseParser & parser_ = TResponseParser{})
        : timeouts{params.timeouts}
        , max_tries{params.max_tries}
        , retry_initial_backoff_ms{params.retry_initial_backoff_ms}
        , retry_max_backoff_ms{params.retry_max_backoff_ms}
        , forward_headers{params.forward_headers.begin(), params.forward_headers.end()}
        , uri{params.uri}
        , parser{parser_}
    {
    }

    Result authenticateRequest(Poco::Net::HTTPRequest & request, const String & request_data) const
    {
        auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts);
        Poco::Net::HTTPResponse response;

        auto milliseconds_to_wait = retry_initial_backoff_ms;
        for (size_t attempt = 0; attempt < max_tries; ++attempt)
        {
            bool last_attempt = attempt + 1 >= max_tries;
            try
            {
                auto request_body = sendHTTPRequest(*session, request);
                if (!request_data.empty())
                    writeString(request_data, *request_body);
                request_body->finalize();

                auto body = receiveHTTPResponse(*session, response);
                return parser.parse(response, body.get());
            }
            catch (const Poco::Exception &) // TODO: make retries smarter
            {
                if (last_attempt)
                    throw;

                sleepForMilliseconds(milliseconds_to_wait);
                milliseconds_to_wait = std::min(milliseconds_to_wait * 2, retry_max_backoff_ms);
            }
        }
        UNREACHABLE();
    }

    const Poco::URI & getURI() const { return uri; }

    const std::set<String, HTTPFieldLess> & getForwardHeaders() const { return forward_headers; }

private:
    const ConnectionTimeouts timeouts;
    const size_t max_tries;
    const size_t retry_initial_backoff_ms;
    const size_t retry_max_backoff_ms;
    const std::set<String, HTTPFieldLess> forward_headers;
    const Poco::URI uri;
    TResponseParser parser;
};


template <typename TResponseParser>
class HTTPAuthClient : private HTTPAuthClientBase<TResponseParser>
{
public:
    static constexpr const char * content_type = "application/json";

    using HTTPAuthClientBase<TResponseParser>::HTTPAuthClientBase;
    using Result = HTTPAuthClientBase<TResponseParser>::Result;
    using Headers = std::map<String, String, HTTPFieldLess>;

    Result authenticateBasic(const String & user_name, const String & password, const Headers & headers = {}, const String & data = {}) const
    {
        auto request_type = data.empty() ? Poco::Net::HTTPRequest::HTTP_GET : Poco::Net::HTTPRequest::HTTP_POST;

        Poco::Net::HTTPRequest request{request_type, this->getURI().getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
        if (!data.empty())
        {
            request.setContentType(content_type);
            request.setContentLength(data.size());
        }
        setHeaders(request, headers);

        Poco::Net::HTTPBasicCredentials basic_credentials{user_name, password};
        basic_credentials.authenticate(request);

        return this->authenticateRequest(request, data);
    }

    Result authenticateBearer(const String & token, const Headers & headers = {}, const String & data = {}) const
    {
        auto request_type = data.empty() ? Poco::Net::HTTPRequest::HTTP_GET : Poco::Net::HTTPRequest::HTTP_POST;

        Poco::Net::HTTPRequest request{request_type, this->getURI().getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
        if (!data.empty())
        {
            request.setContentType(content_type);
            request.setContentLength(data.size());
        }
        setHeaders(request, headers);

        Poco::Net::OAuth20Credentials credentials{token};
        credentials.authenticate(request);

        return this->authenticateRequest(request, data);
    }

private:
    void setHeaders(Poco::Net::HTTPRequest & request, const Headers & headers) const
    {
        for (const auto & [name, value] : headers)
        {
            if (!this->getForwardHeaders().contains(name))
                continue;
            /// Poco writes header values verbatim, so a line break would let the value inject extra headers
            /// (e.g. an `Authorization` placed before ours) into the request to the authentication server.
            if (value.find_first_of(std::string_view("\r\n\0", 3)) != std::string::npos)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Forwarded HTTP header \"{}\" contains CR, LF or NUL", name);
            request.add(name, value);
        }
    }
};

}
