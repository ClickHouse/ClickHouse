#include <string>
#include <Common/Exception.h>
#include <Common/Vault.h>

#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

Vault & Vault::instance()
{
    static Vault ret;
    return ret;
}

void Vault::load(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context_)
{
    reset();

    context = context_;

    url = config.getString(config_prefix + ".url", "");

    if (url.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "url is not given for vault.");

    token = config.getString(config_prefix + ".token", "");

    if (token.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "token is not given for vault.");

    loaded = true;
}

String Vault::readSecret(const String & secret, const String & key)
{
    LOG_DEBUG(log, "readSecret {} {}", secret, key);

    DB::HTTPHeaderEntries headers;
    headers.emplace_back("X-Vault-Token", token);
    Poco::URI uri = Poco::URI(fmt::format("{}/v1/secret/data/{}", url, secret));
    Poco::Net::HTTPBasicCredentials credentials{};

    std::string json_str;

    try
    {
        auto wb = DB::BuilderRWBufferFromHTTP(uri)
                      .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
                      .withMethod(Poco::Net::HTTPRequest::HTTP_GET)
                      .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
                      .withSkipNotFound(false)
                      .withHeaders(headers)
                      .create(credentials);

        readJSONObjectPossiblyInvalid(json_str, *wb);
    }
    catch (const DB::HTTPException & e)
    {
        const auto status = e.getHTTPStatus();
        if (status == Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Secret {} not found in vault.", secret);

        throw;
    }

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var res_json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & root = res_json.extract<Poco::JSON::Object::Ptr>();
    const Poco::JSON::Object::Ptr & data = root->getObject("data");
    const Poco::JSON::Object::Ptr & kv = data->getObject("data");

    if (!kv->has(key))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key {} not found in secret {} of vault.", key, secret);

    const auto value = kv->get(key).extract<String>();

    return value;
}

}
