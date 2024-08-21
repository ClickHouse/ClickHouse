#pragma once
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>

#include <base/sleep.h>
#include <Poco/Net/HTTPBasicCredentials.h>


namespace DB
{

struct HTTPAuthClientParams
{
    Poco::URI uri;
    ConnectionTimeouts timeouts;
    size_t max_tries;
    size_t retry_initial_backoff_ms;
    size_t retry_max_backoff_ms;
};

template <typename TResponseParser>
class HTTPAuthClient
{
public:
    using Result = TResponseParser::Result;

    explicit HTTPAuthClient(const HTTPAuthClientParams & params, const TResponseParser & parser_ = TResponseParser{})
        : timeouts{params.timeouts}
        , max_tries{params.max_tries}
        , retry_initial_backoff_ms{params.retry_initial_backoff_ms}
        , retry_max_backoff_ms{params.retry_max_backoff_ms}
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

private:
    const ConnectionTimeouts timeouts;
    const size_t max_tries;
    const size_t retry_initial_backoff_ms;
    const size_t retry_max_backoff_ms;
    const Poco::URI uri;
    TResponseParser parser;
};


template <typename TResponseParser>
class HTTPBasicAuthClient : private HTTPAuthClient<TResponseParser>
{
public:
    using HTTPAuthClient<TResponseParser>::HTTPAuthClient;
    using Result = HTTPAuthClient<TResponseParser>::Result;

    Result authenticate(const String & user_name, const String & password) const
    {
        Poco::Net::HTTPRequest request{
            Poco::Net::HTTPRequest::HTTP_GET, this->getURI().getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
        Poco::Net::HTTPBasicCredentials basic_credentials{user_name, password};
        basic_credentials.authenticate(request);

        return this->authenticateRequest(request);
    }
};

}
