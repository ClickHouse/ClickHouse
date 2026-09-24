#include <IO/HTTPCommon.h>
#include <Storages/Elasticsearch/ElasticsearchClient.h>
#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/Net/HTTPRequest.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}
    ElasticsearchClient::ElasticsearchClient(ElasticsearchConfiguration config_, ContextPtr context_)
    : config(config_)
    , context(context_)
    , log(getLogger("ElasticsearchClient"))
    {
    }

    ElasticsearchClient::IndexPage ElasticsearchClient::searchIndex() const
    {
        const auto uri = Poco::URI(config.url + "/" + config.index + "/" + "_search");
        Poco::JSON::Object request_body;
        Poco::JSON::Object query;
        query.set("match_all", Poco::JSON::Object());
        request_body.set("query", query);
        std::ostringstream body;  
        request_body.stringify(body);
        auto response_json = sendRequestToElastic(Poco::Net::HTTPRequest::HTTP_POST, uri, body.str());

        return response_json->getObject("hits")->getArray("hits");
    }

    static Poco::JSON::Object::Ptr parseJSONObject(const String & data)
    {
        try
        {
            Poco::JSON::Parser parser;
            auto object = parser.parse(data).extract<Poco::JSON::Object::Ptr>();
            if (!object)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse response");
            return object;
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse response {}", e.what());
        }
    }

    Poco::JSON::Object::Ptr ElasticsearchClient::sendRequestToElastic(const String & method, const Poco::URI & uri, const String & request_body) const
    {
        auto do_request = [&]()
        {
            HTTPHeaderEntries headers;
            ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback;
            if (!request_body.empty())
            {
                headers.emplace_back("Content-Type", "application/json");
                out_stream_callback = [&request_body](std::ostream & os) { os << request_body; };
            }
            auto buf = BuilderRWBufferFromHTTP(uri)
                .withConnectionGroup(HTTPConnectionGroupType::HTTP)
                .withMethod(method)
                .withSettings(context->getReadSettings())
                .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
                .withHostFilter(&context->getRemoteHostFilter())
                .withHeaders(headers)
                .withOutCallback(std::move(out_stream_callback));

            WriteBufferFromOwnString response;
            copyData(*buf, response);
            response.finalize();
            return response.str();
        };

        String response;
        try 
        {
            response = do_request();
        } catch (const Poco::Exception & e) 
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot get respone {}", e.what());
        }

        return parseJSONObject(response);
    }
}
