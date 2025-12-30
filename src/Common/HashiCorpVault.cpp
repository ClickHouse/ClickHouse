#include <IO/HTTPCommon.h>
#include <Interpreters/Context.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

#if USE_SSL
#    include <Poco/Net/Context.h>
#    include <Poco/Net/HTTPSClientSession.h>
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/Utility.h>
#    include <Poco/StringTokenizer.h>
#endif

#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Common/HashiCorpVault.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_PARSE_JSON;
extern const int INVALID_JSON_STRUCTURE;
extern const int SUPPORT_IS_DISABLED;
}

HashiCorpVault & HashiCorpVault::instance()
{
    static HashiCorpVault ret;
    return ret;
}

#if USE_SSL
void HashiCorpVault::initRequestContext(const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    if (!config.has(prefix + ".ssl") && auth_method == HashiCorpVaultAuthMethod::Cert)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ssl section is not specified for vault.");

    std::string ssl_prefix = prefix + ".ssl.";

    Poco::Net::Context::Params params;

    params.privateKeyFile = config.getString(ssl_prefix + Poco::Net::SSLManager::CFG_PRIV_KEY_FILE, "");

    params.certificateFile = config.getString(ssl_prefix + Poco::Net::SSLManager::CFG_CERTIFICATE_FILE, "");

    if (auth_method == HashiCorpVaultAuthMethod::Cert)
    {
        if (params.privateKeyFile.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "privateKeyFile is not specified for vault.");

        if (params.certificateFile.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "certificateFile is not specified for vault.");
    }
    else
    {
        if (!params.certificateFile.empty() && params.privateKeyFile.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "privateKeyFile is not specified for vault.");
    }

    params.caLocation = config.getString(ssl_prefix + Poco::Net::SSLManager::CFG_CA_LOCATION, "");

    params.verificationMode = Poco::Net::SSLManager::VAL_VER_MODE;
    if (config.hasProperty(prefix + Poco::Net::SSLManager::CFG_VER_MODE))
    {
        // either: none, relaxed, strict, once
        std::string mode = config.getString(prefix + Poco::Net::SSLManager::CFG_VER_MODE);
        params.verificationMode = Poco::Net::Utility::convertVerificationMode(mode);
    }

    params.verificationDepth = config.getInt(prefix + Poco::Net::SSLManager::CFG_VER_DEPTH, Poco::Net::SSLManager::VAL_VER_DEPTH);
    params.loadDefaultCAs
        = config.getBool(prefix + Poco::Net::SSLManager::CFG_ENABLE_DEFAULT_CA, Poco::Net::SSLManager::VAL_ENABLE_DEFAULT_CA);
    params.cipherList = config.getString(prefix + Poco::Net::SSLManager::CFG_CIPHER_LIST, Poco::Net::SSLManager::VAL_CIPHER_LIST);
    params.cipherList = config.getString(prefix + Poco::Net::SSLManager::CFG_CYPHER_LIST, params.cipherList); // for backwards compatibility
    bool require_tlsv1 = config.getBool(prefix + Poco::Net::SSLManager::CFG_REQUIRE_TLSV1, false);
    bool require_tlsv1_1 = config.getBool(prefix + Poco::Net::SSLManager::CFG_REQUIRE_TLSV1_1, false);
    bool require_tlsv1_2 = config.getBool(prefix + Poco::Net::SSLManager::CFG_REQUIRE_TLSV1_2, false);
    params.dhParamsFile = config.getString(prefix + Poco::Net::SSLManager::CFG_DH_PARAMS_FILE, "");
    params.ecdhCurve = config.getString(prefix + Poco::Net::SSLManager::CFG_ECDH_CURVE, "");

    Poco::Net::Context::Usage usage;
    if (require_tlsv1_2)
        usage = Poco::Net::Context::TLSV1_2_CLIENT_USE;
    else if (require_tlsv1_1)
        usage = Poco::Net::Context::TLSV1_1_CLIENT_USE;
    else if (require_tlsv1)
        usage = Poco::Net::Context::TLSV1_CLIENT_USE;
    else
        usage = Poco::Net::Context::CLIENT_USE;
    request_context = new Poco::Net::Context(usage, params);

    std::string disabled_protocols_list = config.getString(prefix + Poco::Net::SSLManager::CFG_DISABLE_PROTOCOLS, "");
    Poco::StringTokenizer dp_tok(disabled_protocols_list, ";,", Poco::StringTokenizer::TOK_TRIM | Poco::StringTokenizer::TOK_IGNORE_EMPTY);
    int disabled_protocols = 0;
    for (const auto & dp_token : dp_tok)
    {
        if (dp_token == "sslv2")
            disabled_protocols |= Poco::Net::Context::PROTO_SSLV2;
        else if (dp_token == "sslv3")
            disabled_protocols |= Poco::Net::Context::PROTO_SSLV3;
        else if (dp_token == "tlsv1")
            disabled_protocols |= Poco::Net::Context::PROTO_TLSV1;
        else if (dp_token == "tlsv1_1")
            disabled_protocols |= Poco::Net::Context::PROTO_TLSV1_1;
        else if (dp_token == "tlsv1_2")
            disabled_protocols |= Poco::Net::Context::PROTO_TLSV1_2;
    }

    request_context->disableProtocols(disabled_protocols);

    bool cache_sessions = config.getBool(prefix + Poco::Net::SSLManager::CFG_CACHE_SESSIONS, false);
    request_context->enableSessionCache(cache_sessions);

    // Set to false in order to avoid memory leak
    request_context->enableExtendedCertificateVerification(false);

    bool prefer_server_ciphers = config.getBool(prefix + Poco::Net::SSLManager::CFG_PREFER_SERVER_CIPHERS, false);
    if (prefer_server_ciphers)
        request_context->preferServerCiphers();
}
#endif

void HashiCorpVault::load(const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    reset();

    if (config.has(prefix))
    {
        url = config.getString(prefix + ".url", "");
        if (url.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "url is not specified for vault.");

        try
        {
            Poco::URI uri(url);
            scheme = uri.getScheme();
            host = uri.getHost();
            port = uri.getPort();
        }
        catch (const Poco::Exception &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error in parsing url for vault.");
        }
        if (port == 0)
        {
            if (scheme == "https")
                port = 443;
            else
                port = 80;
        }

        if (config.has(prefix + ".userpass"))
        {
            if (config.has(prefix + ".token"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple auth methods are specified for vault.");

            username = config.getString(prefix + ".userpass.username", "");
            password = config.getString(prefix + ".userpass.password", "");

            if (username.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "username is not specified for vault.");

            auth_method = HashiCorpVaultAuthMethod::Userpass;
        }
        else if (config.has(prefix + ".cert"))
        {
#if USE_SSL
            cert_name = config.getString(prefix + ".cert.name", "");

            // Name of role validation is not required. Because if name of role is not specified then
            // Vault tries all roles and uses any one that matches.

            auth_method = HashiCorpVaultAuthMethod::Cert;
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL is disabled, because ClickHouse was built without SSL library");
#endif
        }
        else
        {
            if (!config.has(prefix + ".token"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Auth sections are not specified for vault.");

            token = config.getString(prefix + ".token", "");

            if (token.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "token is not specified for vault.");

            auth_method = HashiCorpVaultAuthMethod::Token;
        }

        if (scheme == "https")
#if USE_SSL
            initRequestContext(config, prefix);
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL is disabled, because ClickHouse was built without SSL library");
#endif

        loaded = true;
    }
}


String HashiCorpVault::makeRequest(const String & method, const String & path, const String & request_token, const String & body)
{
    std::unique_ptr<Poco::Net::HTTPClientSession> session;

    if (scheme == "https")
    {
#if USE_SSL
        session = std::make_unique<Poco::Net::HTTPSClientSession>(host, port, request_context);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL is disabled, because ClickHouse was built without SSL library");
#endif
    }
    else
        session = std::make_unique<Poco::Net::HTTPClientSession>(host, port);

    session->setTimeout(Poco::Timespan(30, 0));

    Poco::Net::HTTPRequest request(method, path, Poco::Net::HTTPMessage::HTTP_1_1);


    if (!request_token.empty())
    {
        request.set("X-Vault-Token", request_token);
    }

    if (!body.empty())
    {
        request.setContentType("application/json");
        request.setContentLength(body.length());
    }
    std::ostream & os = session->sendRequest(request);
    if (!body.empty())
    {
        os << body;
    }

    Poco::Net::HTTPResponse response;
    std::istream & is = session->receiveResponse(response);

    std::stringstream responseStream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::StreamCopier::copyStream(is, responseStream);

    if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
    {
        throw Poco::Exception("HTTP error: " + std::to_string(response.getStatus()) + " Response: " + responseStream.str());
    }

    std::string value = responseStream.str();

    return value;
}

String HashiCorpVault::login()
{
    std::string json_str;
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Object obj;
    try
    {
        if (auth_method == HashiCorpVaultAuthMethod::Userpass)
        {
            obj.set("password", password);
            Poco::JSON::Stringifier::stringify(obj, oss);
            String uri = fmt::format("/v1/auth/userpass/login/{}", username);
            json_str = makeRequest("POST", uri, "", oss.str());
        }
        else
        {
            // cert auth
            obj.set("name", cert_name);
            Poco::JSON::Stringifier::stringify(obj, oss);
            json_str = makeRequest("POST", "/v1/auth/cert/login", "", oss.str());
        }
    }
    catch (const Poco::Exception & e)
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

    if (auth_method == HashiCorpVaultAuthMethod::Userpass || auth_method == HashiCorpVaultAuthMethod::Cert)
        client_token = login();
    else
        client_token = token;

    std::string json_str;
    try
    {
        json_str = makeRequest("GET", fmt::format("/v1/secret/data/{}", secret), client_token, "");
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
