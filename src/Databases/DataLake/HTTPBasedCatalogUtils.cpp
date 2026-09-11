#include <Databases/DataLake/HTTPBasedCatalogUtils.h>

#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>
#include <Core/Types.h>

namespace DB::ErrorCodes
{
    extern const int DATALAKE_DATABASE_ERROR;
}

namespace DataLake
{

DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
    const std::string & endpoint,
    DB::ContextPtr context,
    const Poco::Net::HTTPBasicCredentials & credentials,
    const Poco::URI::QueryParameters & params,
    const DB::HTTPHeaderEntries & headers,
    const std::string & method,
    std::function<void(std::ostream &)> out_stream_callaback)
{
    Poco::URI url(endpoint);
    if (!params.empty())
        url.setQueryParameters(params);

    return DB::BuilderRWBufferFromHTTP(url)
        .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
        .withSettings(context->getReadSettings())
        .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
        .withHostFilter(&context->getRemoteHostFilter())
        .withHeaders(headers)
        .withDelayInit(false)
        .withSkipNotFound(false)
        .withMethod(method)
        .withOutCallback(out_stream_callaback)
        .create(credentials);
}

std::pair<Poco::Dynamic::Var, std::string> makeHTTPRequestAndReadJSON(
    const std::string & endpoint,
    DB::ContextPtr context,
    const Poco::Net::HTTPBasicCredentials & credentials,
    const Poco::URI::QueryParameters & params,
    const DB::HTTPHeaderEntries & headers,
    const std::string & method,
    std::function<void(std::ostream &)> out_stream_callaback)
{
    auto buf = createReadBuffer(endpoint, context, credentials, params, headers, method, out_stream_callaback);
    if (buf->eof())
        return {};

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);
    Poco::JSON::Parser parser;
    try
    {
        auto result = parser.parse(json_str);
        return std::make_pair(result, json_str);
    }
    catch (const Poco::Exception & poco_ex)
    {

#ifdef DEBUG_OR_SANITIZER_BUILD
        std::string message = poco_ex.displayText() + " Cannot parse json: " + json_str;
#else
        std::string message = "Cannot parse json: " + poco_ex.displayText();
#endif
        throw DB::Exception::createRuntime(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, std::move(message));
    }
}


}
