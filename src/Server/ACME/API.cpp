#include <Server/ACME/API.h>

#if USE_SSL

#include <Common/Base64.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/JSONWebKey.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/ProfileEvents.h>
#include <Disks/IO/ReadBufferFromWebServer.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/StreamCopier.h>


namespace ProfileEvents
{
    extern const Event ACMECertificateOrders;
    extern const Event ACMEAPIRequests;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ACME_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace ACME
{

Directory Directory::parse(const std::string & json_data)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(json_data).extract<Poco::JSON::Object::Ptr>();

    auto dir = Directory{
        .new_account = Poco::URI(json->getValue<std::string>(Directory::new_account_key)),
        .new_order = Poco::URI(json->getValue<std::string>(Directory::new_order_key)),
        .new_nonce = Poco::URI(json->getValue<std::string>(Directory::new_nonce_key)),
    };

    chassert(!dir.new_account.empty());
    chassert(!dir.new_order.empty());
    chassert(!dir.new_nonce.empty());

    LOG_TEST(
        &Poco::Logger::get("ACME::Directory"),
        "Directory: newAccount: {}, newOrder: {}, newNonce: {}",
        dir.new_account.toString(),
        dir.new_order.toString(),
        dir.new_nonce.toString()
    );

    return dir;
}

namespace
{
    std::string readURLUntilEOF(const Poco::URI & url)
    {
        auto * log = &Poco::Logger::get("ACME::API");

        LOG_TEST(log, "Requesting URL: {}", url.toString());

        std::string result;
        ReadSettings read_settings;
        auto reader = std::make_unique<ReadBufferFromWebServer>(
            url.toString(),
            Context::getGlobalContextInstance(),
            DBMS_DEFAULT_BUFFER_SIZE,
            read_settings,
            /* use_external_buffer */ true,
            /* read_until_position */ 0);
        readStringUntilEOF(result, *reader);

        LOG_TEST(log, "Response from URL {}: {}", url.toString(), result);

        return result;
    }
}

API::API(Configuration _configuration): configuration(_configuration)
{
    connection_timeout_settings = ConnectionTimeouts();
    proxy_configuration = ProxyConfiguration();

    directory = getDirectory();
    authenticate();

    LOG_TRACE(&Poco::Logger::get("ACME::API"), "ACME API initialized");
}

DirectoryPtr API::getDirectory() const
{
    LOG_TRACE(log, "Requesting ACME directory from {}", configuration.directory_url.toString());
    auto response = readURLUntilEOF(configuration.directory_url);

    return std::make_shared<Directory>(Directory::parse(response));
}

std::string API::requestNonce() const
{
    if (!directory)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory is not initialized");

    LOG_TEST(log, "Requesting nonce from {}", directory->new_nonce.toString());

    auto uri = Poco::URI(directory->new_nonce);
    auto r = Poco::Net::HTTPRequest(Poco::Net::HTTPRequest::HTTP_HEAD, uri.getPathAndQuery());

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, connection_timeout_settings, proxy_configuration);
    session->sendRequest(r);

    auto response = Poco::Net::HTTPResponse();
    receiveResponse(*session, r, response, /* allow_redirects */ false);

    return response.get(NONCE_HEADER_NAME);
}

std::string API::formatJWSRequestData(const Poco::URI & url, const std::string & payload, const std::string & nonce) const
{
    chassert(configuration.private_key);

    std::string protected_data;
    if (!key_id.empty())
        protected_data = fmt::format(R"({{"alg":"RS256","kid":"{}","nonce":"{}","url":"{}"}})", key_id, nonce, url.toString());
    else
    {
        auto jwk = JSONWebKey::fromRSAKey(*configuration.private_key).toString();
        protected_data = fmt::format(R"({{"alg":"RS256","jwk":{},"nonce":"{}","url":"{}"}})", jwk, nonce, url.toString());
    }

    std::string protected_enc = base64Encode(protected_data, /*url_encoding*/ true, /*no_padding*/ true);

    auto payload_enc = base64Encode(payload, /*url_encoding*/ true, /*no_padding*/ true);

    EVP_PKEY * pkey = static_cast<EVP_PKEY*>(*configuration.private_key);
    std::string to_sign = fmt::format("{}.{}", protected_enc, payload_enc);
    std::string signature = rsaSHA256Sign(pkey, to_sign);
    std::string encoded_signature = base64Encode(signature, /*url_encoding*/ true, /*no_padding*/ true);

    return
        R"({"protected":")" + protected_enc
        + R"(","payload":")" + payload_enc
        + R"(","signature":")" + encoded_signature
        + R"("})";
}

std::string API::doJWSRequest(
    const Poco::URI & url,
    const std::string & payload,
    std::shared_ptr<Poco::Net::HTTPResponse> response) const
{
    LOG_TEST(log, "Making JWS request to URL: {} with payload {}", url.toString(), payload);

    std::string nonce = requestNonce();
    std::string request_data = formatJWSRequestData(url, payload, nonce);

    auto r = Poco::Net::HTTPRequest(Poco::Net::HTTPRequest::HTTP_POST, url.getPathAndQuery());
    r.set("Content-Type", APPLICATION_JOSE_JSON);
    r.set("Content-Length", std::to_string(request_data.size()));

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, url, connection_timeout_settings, proxy_configuration);

    ProfileEvents::increment(ProfileEvents::ACMEAPIRequests);

    auto & ostream = session->sendRequest(r);
    ostream << request_data;

    if (!response)
        response = std::make_shared<Poco::Net::HTTPResponse>();

    auto * rstream = receiveResponse(*session, r, *response, /* allow_redirects */ false);

    std::string response_data;
    Poco::StreamCopier::copyToString(*rstream, response_data);

    return response_data;
}

Poco::JSON::Object::Ptr API::doJWSRequestExpectingJSON(
    const Poco::URI & url,
    const std::string & payload,
    std::shared_ptr<Poco::Net::HTTPResponse> response) const
{
    auto response_data = doJWSRequest(url, payload, response);

    Poco::JSON::Parser parser;
    return parser.parse(response_data).extract<Poco::JSON::Object::Ptr>();
}

std::string API::doJWSRequestWithEmptyPayload(const Poco::URI & url) const
{
    auto http_response = std::make_shared<Poco::Net::HTTPResponse>();
    return doJWSRequest(url, /*payload=*/ "", http_response);
}

std::string API::authenticate()
{
    if (!key_id.empty())
        return key_id;

    std::string payload;

    if (!configuration.contact_email.empty())
        payload = fmt::format(
            R"({{"contact":["mailto:{}"],"termsOfServiceAgreed":{}}})",
            configuration.contact_email,
            configuration.terms_of_service_agreed ? "true" : "false");
    else
        payload = fmt::format(R"({{"termsOfServiceAgreed":{}}})", configuration.terms_of_service_agreed);

    auto http_response = std::make_shared<Poco::Net::HTTPResponse>();
    auto json = doJWSRequestExpectingJSON(directory->new_account, payload, http_response);

    if (!json->has("status") || json->getValue<std::string>("status") != "valid")
        throw Exception(ErrorCodes::ACME_ERROR, "Account creation failed");

    key_id = (*http_response).get("Location");

    return key_id;
}

std::string API::order(const Domains & domains, OrderCallback callback) const
{
    std::string payload_from_domains;
    {
        Poco::JSON::Object payload_json_object;
        auto payload_identifiers = Poco::JSON::Array();

        for (const auto & domain : domains)
        {
            Poco::JSON::Object identifier;
            identifier.set("type", "dns");
            identifier.set("value", domain);
            payload_identifiers.add(identifier);
        }

        payload_json_object.set("identifiers", payload_identifiers);

        std::ostringstream payload; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(payload_json_object, payload);

        payload_from_domains = payload.str();
    }

    auto http_response = std::make_shared<Poco::Net::HTTPResponse>();
    auto json = doJWSRequestExpectingJSON(directory->new_order, payload_from_domains, http_response);

    ProfileEvents::increment(ProfileEvents::ACMECertificateOrders);

    auto status = json->getValue<std::string>("status");
    auto expires = json->getValue<std::string>("expires");
    auto authorizations = json->getArray("authorizations");
    auto finalize = json->getValue<std::string>("finalize");

    LOG_TEST(log, "Status: {}, Expires: {}, Finalize: {}", status, expires, finalize);

    auto order_url = http_response->get("Location");

    for (const auto & auth : *authorizations)
        processAuthorization(Poco::URI(auth.toString()), callback);

    return order_url;
}

Order API::describeOrder(const Poco::URI & order_url) const
{
    auto http_response = std::make_shared<Poco::Net::HTTPResponse>();
    auto json = doJWSRequestExpectingJSON(order_url, "", http_response);

    auto status = json->getValue<std::string>("status");
    auto finalize = json->getValue<std::string>("finalize");

    std::string certificate;
    if (json->has("certificate"))
        certificate = json->getValue<std::string>("certificate");

    return Order{
        .status = status,
        .order_url = order_url,
        .finalize_url = Poco::URI(finalize),
        .certificate_url = Poco::URI(certificate),
    };
}

std::string API::pullCertificate(const Poco::URI & certificate_url) const
{
    return doJWSRequestWithEmptyPayload(certificate_url);
}

bool API::finalizeOrder(const Poco::URI & finalize_url, const Domains & domains, const KeyPair & pkey) const
{
    EVP_PKEY * key = static_cast<EVP_PKEY *>(pkey);

    std::string csr = generateCSR(domains, key);
    auto payload = R"({"csr":")" + csr + R"("})";

    doJWSRequest(finalize_url, payload, nullptr);

    return true;
}

void API::processAuthorization(const Poco::URI & auth_url, OrderCallback callback) const
{
    auto http_response = std::make_shared<Poco::Net::HTTPResponse>();
    auto json = doJWSRequestExpectingJSON(auth_url, "", http_response);

    for (const auto & challenge : *json->getArray("challenges"))
    {
        const auto & ch = challenge.extract<Poco::JSON::Object::Ptr>();

        if (ch->has("validated"))
            continue;

        auto type = ch->getValue<std::string>("type");
        auto url = ch->getValue<std::string>("url");
        auto status = ch->getValue<std::string>("status");
        auto token = ch->getValue<std::string>("token");

        LOG_TEST(log, "Challenge: type: {}, url: {}, status: {}, token: {}", type, url, status, token);
        if (type == HTTP_01_CHALLENGE_TYPE)
        {
            if (status == "valid")
                continue;

            callback(token);

            doJWSRequest(Poco::URI(url), "{}", nullptr);
        }
    }
}

}
}
#endif
