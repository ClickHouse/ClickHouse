#include <string>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>
#include <base/JSON.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>
#include <Common/Exception.h>
#include <Common/HashiCorpVault.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_PARSE_JSON;
extern const int INVALID_JSON_STRUCTURE;
}

HashiCorpVault & HashiCorpVault::instance()
{
    static HashiCorpVault ret;
    return ret;
}

void HashiCorpVault::load(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context_)
{
    reset();

    context = context_;

    if (config.has(config_prefix))
    {
        url = config.getString(config_prefix + ".url", "");

        if (url.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "url is not specified for vault.");

        if (config.has(config_prefix + ".userpass"))
        {
            if (config.has(config_prefix + ".token"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple auth methods are specified for vault.");

            username = config.getString(config_prefix + ".userpass.username", "");
            password = config.getString(config_prefix + ".userpass.password", "");

            if (username.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "username is not specified for vault.");

            auth_method = HashiCorpVaultAuthMethod::Userpass;
        } else {
            if (!config.has(config_prefix + ".token"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Auth sections are not specified for vault.");

            token = config.getString(config_prefix + ".token", "");

            if (token.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "token is not specified for vault.");

            auth_method = HashiCorpVaultAuthMethod::Token;
        }

        loaded = true;
    }
}

String HashiCorpVault::login()
{
    DB::HTTPHeaderEntries headers;

    headers.emplace_back("X-Vault-Token", token);
    headers.emplace_back("Content-Type", "application/json");

    Poco::URI uri = Poco::URI(fmt::format("{}/v1/auth/userpass/login/{}", url, username));
    Poco::Net::HTTPBasicCredentials credentials{};

    std::string json_str;

    try
    {
        auto wb = DB::BuilderRWBufferFromHTTP(uri)
                      .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
                      .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                      .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
                      .withSkipNotFound(false)
                      .withHeaders(headers)
                      .withOutCallback(
                          [this](std::ostream & os)
                          {
                              Poco::JSON::Object obj;
                              obj.set("password", password);
                              obj.stringify(os);
                          })
                      .create(credentials);

        readJSONObjectPossiblyInvalid(json_str, *wb);
    }
    catch (const DB::HTTPException & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot login in vault as {}. ({})", username, e.displayText());
    }

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var res_json;
    try
    {
        res_json = parser.parse(json_str);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::CANNOT_PARSE_JSON, "Cannot parse JSON response from vault. ({})", e.displayText());
    }

    try
    {
        const Poco::JSON::Object::Ptr & root = res_json.extract<Poco::JSON::Object::Ptr>();
        const Poco::JSON::Object::Ptr & auth = root->getObject("auth");
        const auto value = auth->get("client_token").extract<String>();

        return value;
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::INVALID_JSON_STRUCTURE, "Invalid JSON structure in response from vault. ({})", e.displayText());
    }
}

String HashiCorpVault::readSecret(const String & secret, const String & key)
{
    LOG_DEBUG(log, "readSecret {} {}", secret, key);

    if (auth_method == HashiCorpVaultAuthMethod::Userpass)
        token = login();

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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Secret {} not found in vault. ({})", secret, e.displayText());

        throw;
    }

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var res_json;
    try
    {
        res_json = parser.parse(json_str);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::CANNOT_PARSE_JSON, "Cannot parse JSON response from vault. ({})", e.displayText());
    }

    try
    {
        const Poco::JSON::Object::Ptr & root = res_json.extract<Poco::JSON::Object::Ptr>();
        const Poco::JSON::Object::Ptr & data = root->getObject("data");
        const Poco::JSON::Object::Ptr & kv = data->getObject("data");

        if (!kv->has(key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key {} not found in secret {} of vault.", key, secret);

        const auto value = kv->get(key).extract<String>();

        return value;
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::INVALID_JSON_STRUCTURE, "Invalid JSON structure in response from vault. ({})", e.displayText());
    }
}

}
