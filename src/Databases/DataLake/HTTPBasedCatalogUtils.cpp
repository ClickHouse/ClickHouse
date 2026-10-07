#include <Databases/DataLake/HTTPBasedCatalogUtils.h>

#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>
#include <Core/Types.h>
#include <Common/FailPoint.h>
#include <Common/HTTPHeaderFilter.h>

namespace DB::ErrorCodes
{
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int FAULT_INJECTED;
}

namespace DB::FailPoints
{
    extern const char check_database_datalake_negative[];
}

namespace DataLake
{

void validateBearerToken(const DB::ContextPtr & context, const std::string & bearer_token)
{
    /// `createWithBearerToken` turns a non-empty token into an `Authorization: Bearer <token>`
    /// header. Validate that synthetic header the same way a user-supplied `auth_header` is
    /// validated, so a token cannot inject additional headers (an embedded newline) or send an
    /// `Authorization` header that `http_forbid_headers` forbids. An empty token sends no header.
    if (bearer_token.empty())
        return;

    DB::HTTPHeaderEntries auth_header{{"Authorization", "Bearer " + bearer_token}};
    context->getGlobalContext()->getHTTPHeaderFilter().checkAndNormalizeHeaders(auth_header);
}

DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
    const std::string & endpoint,
    DB::ContextPtr context,
    const std::string & bearer_token,
    const Poco::URI::QueryParameters & params,
    const DB::HTTPHeaderEntries & headers,
    const std::string & method,
    std::function<void(std::ostream &)> out_stream_callaback)
{
    validateBearerToken(context, bearer_token);

    Poco::URI url(endpoint);
    if (!params.empty())
        url.setQueryParameters(params);

    /// Catalogs authenticate with a bearer token; there are no HTTP Basic credentials

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
        .createWithBearerToken(bearer_token);
}

std::pair<Poco::Dynamic::Var, std::string> makeHTTPRequestAndReadJSON(
    const std::string & endpoint,
    DB::ContextPtr context,
    const std::string & bearer_token,
    const Poco::URI::QueryParameters & params,
    const DB::HTTPHeaderEntries & headers,
    const std::string & method,
    std::function<void(std::ostream &)> out_stream_callaback)
{
    fiu_do_on(DB::FailPoints::check_database_datalake_negative,
    {
        throw DB::Exception(DB::ErrorCodes::FAULT_INJECTED, "Injecting fault when checking database");
    });

    auto buf = createReadBuffer(endpoint, context, bearer_token, params, headers, method, out_stream_callaback);
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
