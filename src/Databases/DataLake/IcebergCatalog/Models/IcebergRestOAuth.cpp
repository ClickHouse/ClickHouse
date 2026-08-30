#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestOAuth.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DataLake::IcebergRestModels
{

OAuthTokenResponse parseOAuthTokenResponse(const std::string & json, bool require_bearer_type)
{
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var parsed = parser.parse(json);
    const auto & object = parsed.extract<Poco::JSON::Object::Ptr>();

    if (!object->has("access_token"))
    {
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "OAuth token response has no `access_token` field: {}",
            json);
    }

    OAuthTokenResponse result;
    result.access_token = object->get("access_token").extract<std::string>();

    if (object->has("expires_in"))
        result.expires_in = object->getValue<int64_t>("expires_in");

    if (object->has("token_type"))
        result.token_type = object->getValue<std::string>("token_type");

    if (require_bearer_type && result.token_type != "Bearer")
    {
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Unexpected token type in OAuth response. Expected Bearer token, got {}",
            result.token_type);
    }

    return result;
}

}

#endif
