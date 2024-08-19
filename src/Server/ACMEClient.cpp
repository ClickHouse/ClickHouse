#include <Server/ACMEClient.h>

#include <Core/BackgroundSchedulePool.h>
#include <Disks/IO/ReadBufferFromWebServer.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/Credentials.h>
#include <Interpreters/Context.h>
#include <fmt/core.h>
#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <Poco/Base64Encoder.h>
#include <Poco/Crypto/CryptoStream.h>
#include <Poco/Crypto/ECKey.h>
#include <Poco/Crypto/RSADigestEngine.h>
#include <Poco/Crypto/RSAKey.h>
#include <Poco/Crypto/RSAKeyImpl.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Poco/DateTimeFormatter.h>
#include <Poco/DigestEngine.h>
#include <Poco/File.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/SHA1Engine.h>
#include <Poco/StreamCopier.h>
#include <Poco/String.h>
#include <Poco/URI.h>
#include "Common/OpenSSLHelpers.h"
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include "base/sleep.h"


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{


namespace ACMEClient
{

namespace
{

namespace fs = std::filesystem;

// void dumberCallback(const std::string & domain_name, const std::string & url, const std::string & key)
// {
//     /// Callback for domain thevar1able.com with url
//     /// http://thevar1able.com/.well-known/acme-challenge/TU9yW7Ad6RgzrmILfp8Zyn9swtxhYrlMYAXVQe29fPU
//     /// and key TU9yW7Ad6RgzrmILfp8Zyn9swtxhYrlMYAXVQe29fPU.A1qzB0q34e_9tysLnbHKvnXAj49583OrPNUL7cPbC5Q
//
//     ACMEClient::instance().dummyCallback(domain_name, url, key);
// }
std::string generateCSR()
{
    std::string name = "letsencrypt-stg.var1able.network";

    auto bits = 4096;
    EVP_PKEY * key(EVP_RSA_gen(bits));

    X509_REQ * req(X509_REQ_new());
    X509_NAME * cn = X509_REQ_get_subject_name(req);
    if (!X509_NAME_add_entry_by_txt(cn, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char *>(name.c_str()), -1, -1, 0))
    {
        throw std::runtime_error("Error adding CN to X509_NAME");
    }

    BIO * key_bio(BIO_new(BIO_s_mem()));
    if (PEM_write_bio_PrivateKey(key_bio, key, nullptr, nullptr, 0, nullptr, nullptr) != 1)
    {
        throw std::runtime_error("Error writing private key to BIO");
    }
    std::string private_key;


    if (!X509_REQ_set_pubkey(req, key))
    {
        throw std::runtime_error("Failure in X509_REQ_set_pubkey");
    }

    if (!X509_REQ_sign(req, key, EVP_sha256()))
    {
        throw std::runtime_error("Failure in X509_REQ_sign");
    }

    BIO * req_bio(BIO_new(BIO_s_mem()));
    if (i2d_X509_REQ_bio(req_bio, req) < 0)
    {
        throw std::runtime_error("Failure in i2d_X509_REQ_bio");
    }

    std::string csr;
    char buffer[1024];
    int bytes_read;
    while ((bytes_read = BIO_read(req_bio, buffer, sizeof(buffer))) > 0)
    {
        LOG_DEBUG(&Poco::Logger::get("ACME"), "Read {} bytes", bytes_read);
        csr.append(buffer, bytes_read);
    }
    csr = base64Encode(csr, /*url_encoding*/ true, /*no_padding*/ true);

    while ((bytes_read = BIO_read(key_bio, buffer, sizeof(buffer))) > 0)
    {
        private_key.append(buffer, bytes_read);
    }

    LOG_DEBUG(&Poco::Logger::get("ACME"), "CSR: {}", csr);
    LOG_DEBUG(&Poco::Logger::get("ACME"), "Private key: {}", private_key);

    return csr;
}


}


ACMEClient & ACMEClient::instance()
{
    static ACMEClient instance;
    return instance;
}

void ACMEClient::requestCertificate(const Poco::Util::AbstractConfiguration &)
{
    /// TODO
}

void ACMEClient::reload(const Poco::Util::AbstractConfiguration & config)
try
{
    auto context = Context::getGlobalContextInstance();

    // auto http_port = config.getInt("http_port");
    // if (http_port != 80)
    //     throw Exception(ErrorCodes::LOGICAL_ERROR, "For ACME HTTP challenge HTTP port must be 80, but is {}", http_port);

    auto now = Poco::DateTimeFormatter::format(Poco::Timestamp(), Poco::DateTimeFormat::ISO8601_FORMAT);
    LOG_DEBUG(log, "Current time: {}", now);

    /// FIXME don't re-read config on each reload
    auto acme_url = config.getString("acme.url", "https://acme-staging-v02.api.letsencrypt.org");
    LOG_DEBUG(log, "ACME URL: {}", acme_url);

    auto acme_email = config.getString("acme.email", "admin@example.com");
    LOG_DEBUG(log, "ACME Email: {}", acme_email);

    auto zk = context->getZooKeeper();
    zk->createIfNotExists(fs::path(ZOOKEEPER_ACME_BASE_PATH), "");

    BackgroundSchedulePool & bgpool = context->getSchedulePool();

    election_task = bgpool.createTask(
        "ACMEClient",
        [this, zk]
        {
            LOG_DEBUG(log, "Running election task");

            election_task->scheduleAfter(1000);

            /// fixme no throwing in BgSchPool
            auto leader = zkutil::EphemeralNodeHolder::tryCreate(fs::path(ZOOKEEPER_ACME_BASE_PATH) / "leader", *zk);
            if (leader)
                leader_node = std::move(leader);

            LOG_DEBUG(log, "I'm the leader: {}", leader_node ? "yes" : "no");
        });
    election_task->activateAndSchedule();

    refresh_task = bgpool.createTask(
        "ACMEClient",
        [this]
        {
            LOG_DEBUG(log, "Running ACMEClient task");

            refresh_task->scheduleAfter(1000);
        });

    refresh_task->activateAndSchedule();

    getDirectory();

    auto key = Poco::Crypto::RSAKey("", "/home/thevar1able/src/clickhouse/cmake-build-debug/acme/acme.key", "");
    private_acme_key = std::make_shared<Poco::Crypto::RSAKey>(key);

    if (!authenticated || key_id.empty())
    {
        authenticate();
        authenticated = true;
    }

    if (!initialized)
    {
        auto order_url = order();
        // do some kind of event loop here
        // describe order -> finalize -> wait
        // describe order -> pull cert
        // finalizeOrder(fin);
        // sleepForSeconds(5);

        // auto response = doJWSRequest(fin, "", nullptr);
        // LOG_DEBUG(log, "Finalize response: {}", response);
    }

    /// TODO on initialization go through config and try to reissue all
    /// expiring/missing certificates.

    initialized = true;
}
catch (...)
{
    tryLogCurrentException("Failed :(");
}


void ACMEClient::getDirectory()
{
    LOG_DEBUG(log, "Requesting ACME directory from {}", "https://acme-staging-v02.api.letsencrypt.org/directory");

    std::string directory_buffer;
    ReadSettings read_settings;
    auto reader = std::make_unique<ReadBufferFromWebServer>(
        "https://acme-staging-v02.api.letsencrypt.org/directory", /// FIXME to const
        Context::getGlobalContextInstance(),
        DBMS_DEFAULT_BUFFER_SIZE,
        read_settings,
        /* use_external_buffer */ true,
        /* read_until_position */ 0);
    readStringUntilEOF(directory_buffer, *reader);

    try
    {
        Poco::JSON::Parser parser;
        auto json = parser.parse(directory_buffer).extract<Poco::JSON::Object::Ptr>();

        auto dir = Directory{
            .new_account = json->getValue<std::string>(Directory::new_account_key),
            .new_order = json->getValue<std::string>(Directory::new_order_key),
            .new_nonce = json->getValue<std::string>(Directory::new_nonce_key),
        };
        directory = std::make_shared<Directory>(dir);

        LOG_DEBUG(log, "Directory: newAccount: {}, newOrder: {}, newNonce: {}", dir.new_account, dir.new_order, dir.new_nonce);
    }
    catch (Poco::Exception & e)
    {
        LOG_WARNING(log, "Can't parse ACME directory: {}", e.displayText());
    }
}

/// todo maybe move somewhere else
std::string hmacSha256(std::string to_sign, Poco::Crypto::RSAKey & key)
{
    EVP_PKEY * pkey = EVP_PKEY_new();
    auto ret = EVP_PKEY_assign_RSA(pkey, key.impl()->getRSA());
    if (ret != 1)
    {
        throw std::runtime_error("Error assigning RSA key to EVP_PKEY");
    }

    size_t signature_length = 0;

    EVP_MD_CTX * context(EVP_MD_CTX_create());
    const EVP_MD * sha256 = EVP_get_digestbyname("SHA256");
    if (!sha256 || EVP_DigestInit_ex(context, sha256, nullptr) != 1 || EVP_DigestSignInit(context, nullptr, sha256, nullptr, pkey) != 1
        || EVP_DigestSignUpdate(context, to_sign.c_str(), to_sign.size()) != 1
        || EVP_DigestSignFinal(context, nullptr, &signature_length) != 1)
    {
        throw std::runtime_error("Error creating SHA256 digest");
    }

    std::vector<unsigned char> signature(signature_length);
    if (EVP_DigestSignFinal(context, &signature.front(), &signature_length) != 1)
    {
        throw std::runtime_error("Error creating SHA256 digest in final signature");
    }

    std::string signature_str = std::string(signature.begin(), signature.end());

    return base64Encode(signature_str, /*url_encoding*/ true, /*no_padding*/ true);
}

std::string formProtectedData(const std::string & jwk, const std::string & nonce, const std::string & url, const std::string & key_id)
{
    auto protected_data = fmt::format(R"({{"alg":"RS256","jwk":{},"nonce":"{}","url":"{}"}})", jwk, nonce, url);

    if (!key_id.empty())
        protected_data = fmt::format(R"({{"alg":"RS256","kid":"{}","nonce":"{}","url":"{}"}})", key_id, nonce, url);

    LOG_DEBUG(&Poco::Logger::get("ACME"), "Protected data: {}", protected_data);

    return base64Encode(protected_data, /*url_encoding*/ true, /*no_padding*/ true);
}

std::string
ACMEClient::doJWSRequest(const std::string & url, const std::string & payload, std::shared_ptr<Poco::Net::HTTPResponse> response)
{
    const auto nonce = requestNonce();

    auto uri = Poco::URI(url);

    auto jwk = JSONWebKey::fromRSAKey(*private_acme_key).toString();
    auto protected_enc = formProtectedData(jwk, nonce, uri.toString(), key_id);

    auto payload_enc = base64Encode(payload, /*url_encoding*/ true, /*no_padding*/ true);

    std::string to_sign = protected_enc + "." + payload_enc;
    auto signature = hmacSha256(to_sign, *private_acme_key);

    std::string request_data
        = R"({"protected":")" + protected_enc + R"(","payload":")" + payload_enc + R"(","signature":")" + signature + R"("})";


    auto r = Poco::Net::HTTPRequest(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery());
    r.set("Content-Type", "application/jose+json");
    r.set("Content-Length", std::to_string(request_data.size()));

    auto timeout = ConnectionTimeouts();
    auto proxy = ProxyConfiguration();
    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeout, proxy);

    auto & ostream = session->sendRequest(r);
    ostream << request_data;

    if (!response)
        response = std::make_shared<Poco::Net::HTTPResponse>();

    auto * rstream = receiveResponse(*session, r, *response, /* allow_redirects */ false);

    std::string response_str;
    Poco::StreamCopier::copyToString(*rstream, response_str);

    // Poco::JSON::Parser parser;
    // auto json = parser.parse(response_str).extract<Poco::JSON::Object::Ptr>();

    return response_str;
}

void ACMEClient::authenticate()
{
    if (!key_id.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Already authenticated");

    std::string payload = R"({"termsOfServiceAgreed":true,"contact":["mailto:admin@example.org"]})";

    auto http_response = std::make_shared<Poco::Net::HTTPResponse>();
    auto response = doJWSRequest(directory->new_account, payload, http_response);

    Poco::JSON::Parser parser;
    auto json = parser.parse(response).extract<Poco::JSON::Object::Ptr>();

    LOG_DEBUG(log, "Account response: {}", response);

    key_id = (*http_response).get("Location");
    for (const auto & [k, v] : *http_response)
    {
        LOG_DEBUG(log, "Response header: {} -> {}", k, v);
    }
}

std::string ACMEClient::order()
{
    std::string payload = R"({"identifiers":[{"type":"dns","value":"letsencrypt-stg.var1able.network"}]})";

    auto http_response = std::make_shared<Poco::Net::HTTPResponse>();
    auto response = doJWSRequest(directory->new_order, payload, http_response);

    Poco::JSON::Parser parser;
    auto json = parser.parse(response).extract<Poco::JSON::Object::Ptr>();

    LOG_DEBUG(log, "Order response: {}", response);

    auto status = json->getValue<std::string>("status");
    auto expires = json->getValue<std::string>("expires");

    auto identifiers = json->getArray("identifiers");
    auto authorizations = json->getArray("authorizations");

    auto finalize = json->getValue<std::string>("finalize");

    LOG_DEBUG(log, "Status: {}, Expires: {}, Finalize: {}", status, expires, finalize);

    auto order_url = http_response->get("Location");
    return order_url;

    // if (status == "ready")
    // {
    //     return order_url;
    // }
    //
    // for (const auto & auth : *authorizations)
    // {
    //     LOG_DEBUG(log, "Authorization: {}", auth.toString());
    //
    //     processAuthorization(auth.toString());
    // }
    //
    // return finalize;
}

void ACMEClient::processAuthorization(const std::string & auth_url)
{
    std::string read_buffer;
    ReadSettings read_settings;
    auto reader = std::make_unique<ReadBufferFromWebServer>(
        auth_url,
        Context::getGlobalContextInstance(),
        DBMS_DEFAULT_BUFFER_SIZE,
        read_settings,
        /* use_external_buffer */ true,
        /* read_until_position */ 0);
    readStringUntilEOF(read_buffer, *reader);

    Poco::JSON::Parser parser;
    auto json = parser.parse(read_buffer).extract<Poco::JSON::Object::Ptr>();

    LOG_DEBUG(log, "Authorization response: {}", read_buffer);

    auto challenges = json->getArray("challenges");

    for (const auto & challenge : *challenges)
    {
        auto ch = challenge.extract<Poco::JSON::Object::Ptr>();

        auto type = ch->getValue<std::string>("type");
        auto url = ch->getValue<std::string>("url");
        auto status = ch->getValue<std::string>("status");
        auto token = ch->getValue<std::string>("token");

        LOG_DEBUG(log, "Challenge: type: {}, url: {}, status: {}, token: {}", type, url, status, token);
        if (type == HTTP_01_CHALLENGE_TYPE)
        {
            if (status == "valid")
                continue;

            auto response = doJWSRequest(url, "{}", nullptr);

            LOG_DEBUG(log, "Response: {}", response);
        }
    }
}

void ACMEClient::finalizeOrder(const std::string & finalize_url)
{
    std::string csr = generateCSR();
    auto payload = R"({"csr":")" + csr + R"("})";

    auto http_response = std::make_shared<Poco::Net::HTTPResponse>();
    auto response = doJWSRequest(finalize_url, payload, http_response);

    LOG_DEBUG(log, "finalizeOrder Response: {}", response);

    for (const auto & [k, v] : *http_response)
    {
        LOG_DEBUG(log, "Response header: {} -> {}", k, v);
    }
}

std::string ACMEClient::requestNonce()
{
    if (!directory)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory is not initialized");

    auto uri = Poco::URI(directory->new_nonce);
    auto r = Poco::Net::HTTPRequest("HEAD", uri.getPathAndQuery());
    LOG_DEBUG(log, "Requesting nonce from {}", uri.getPathAndQuery());

    auto timeout = ConnectionTimeouts();
    auto proxy = ProxyConfiguration();
    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeout, proxy);

    session->sendRequest(r);

    auto response = Poco::Net::HTTPResponse();
    receiveResponse(*session, r, response, /* allow_redirects */ false);

    auto nonce = response.get("replay-nonce");

    return nonce;
}

std::string ACMEClient::requestChallenge(const std::string & uri)
{
    /// TODO go to Keeper and query existing challenge for domain
    /// if not present, return "".

    LOG_DEBUG(log, "Requesting challenge for {}", uri);

    // {
    //  "protected": base64url({
    //    "alg": "ES256",
    //    "kid": "https://example.com/acme/acct/evOfKhNU60wg",
    //    "nonce": "UQI1PoRi5OuXzxuX7V7wL0",
    //    "url": "https://example.com/acme/chall/prV_B7yEyA4"
    //  }),
    //  "payload": base64url({}),
    //  "signature": "Q1bURgJoEslbD1c5...3pYdSMLio57mQNN4"
    // }

    /// TODO remember which tokens we've requested

    if (!uri.starts_with(ACME_CHALLENGE_HTTP_PATH))
        return "";

    std::string token_from_uri = uri.substr(std::string(ACME_CHALLENGE_HTTP_PATH).size());
    auto jwk = JSONWebKey::fromRSAKey(*private_acme_key).toString();
    auto jwk_thumbprint = encodeSHA256(jwk);
    jwk_thumbprint = base64Encode(jwk_thumbprint, /*url_encoding*/ true, /*no_padding*/ true);

    return token_from_uri + "." + jwk_thumbprint;
}

}
}
