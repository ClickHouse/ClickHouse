#pragma once
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>

#include <base/sleep.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/OAuth20Credentials.h>

#include <set>

namespace DB
{

struct HTTPAuthClientParams
{
    Poco::URI uri;
    ConnectionTimeouts timeouts;
    size_t max_tries;
    size_t retry_initial_backoff_ms;
    size_t retry_max_backoff_ms;
    std::vector<String> forward_headers;
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

    Result authenticateRequest(Poco::Net::HTTPRequest & request) const
    {
        auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeouts);
        Poco::Net::HTTPResponse response;

        auto milliseconds_to_wait = retry_initial_backoff_ms;
        for (size_t attempt = 0; attempt < max_tries; ++attempt)
        {
            bool last_attempt = attempt + 1 >= max_tries;
            try
            {
                session->sendRequest(request);
                auto & body_stream = session->receiveResponse(response);
                return parser.parse(response, &body_stream);
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

    struct ci_less
    {
        bool operator()(const std::string & a, const std::string & b) const
        {
            return std::lexicographical_compare(
                a.begin(), a.end(),
                b.begin(), b.end(),
                [](unsigned char ac, unsigned char bc)
                {
                    return std::tolower(ac) < std::tolower(bc);
                });
        }
    };

    const std::set<String, ci_less> & getForwardHeaders() const { return forward_headers; }

private:
    const ConnectionTimeouts timeouts;
    const size_t max_tries;
    const size_t retry_initial_backoff_ms;
    const size_t retry_max_backoff_ms;
    const std::set<String, ci_less> forward_headers;
    const Poco::URI uri;
    TResponseParser parser;
};


template <typename TResponseParser>
class HTTPAuthClient : private HTTPAuthClientBase<TResponseParser>
{
public:
    using HTTPAuthClientBase<TResponseParser>::HTTPAuthClientBase;
    using Result = HTTPAuthClientBase<TResponseParser>::Result;

    Result authenticateBasic(const String & user_name, const String & password, const std::unordered_map<String, String> & headers) const
    {
        Poco::Net::HTTPRequest request{
            Poco::Net::HTTPRequest::HTTP_GET, this->getURI().getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
        setHeaders(request, headers);

        Poco::Net::HTTPBasicCredentials basic_credentials{user_name, password};
        basic_credentials.authenticate(request);

        return this->authenticateRequest(request);
    }

    Result authenticateBearer(const String & token, const std::unordered_map<String, String> & headers) const
    {
        Poco::Net::HTTPRequest request{
            Poco::Net::HTTPRequest::HTTP_GET, this->getURI().getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
        setHeaders(request, headers);

        Poco::Net::OAuth20Credentials credentials{token};
        credentials.authenticate(request);

        return this->authenticateRequest(request);
    }

private:
    void setHeaders(Poco::Net::HTTPRequest & request, const std::unordered_map<String, String> & headers) const
    {
        for (const auto & [name, value] : headers)
        {
            if (this->getForwardHeaders().contains(name))
                request.add(name, value);
        }
    }
};

}
