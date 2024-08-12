#include <Server/ACMEClient.h>

#include "Common/Base64.h"
#include "Common/HTTPConnectionPool.h"
#include "Common/ZooKeeper/ZooKeeper.h"
#include <Common/logger_useful.h>
#include "Core/BackgroundSchedulePool.h"
#include "Disks/IO/ReadBufferFromWebServer.h"
#include "IO/HTTPCommon.h"
#include "IO/ReadWriteBufferFromHTTP.h"
#include "Interpreters/Context.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/Credentials.h>
#include <Poco/Base64Encoder.h>
#include <Poco/Crypto/CryptoStream.h>
#include <Poco/Crypto/RSADigestEngine.h>
#include <Poco/Crypto/RSAKey.h>
#include <Poco/Crypto/RSAKeyImpl.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Poco/DigestEngine.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/SHA1Engine.h>
#include <Poco/String.h>
#include <Poco/URI.h>
#include <fmt/core.h>
#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>


namespace DB
{


namespace ACMEClient
{

namespace
{

namespace fs = std::filesystem;

void dumberCallback(const std::string & domain_name, const std::string & url, const std::string & key)
{
    /// Callback for domain thevar1able.com with url
    /// http://thevar1able.com/.well-known/acme-challenge/TU9yW7Ad6RgzrmILfp8Zyn9swtxhYrlMYAXVQe29fPU
    /// and key TU9yW7Ad6RgzrmILfp8Zyn9swtxhYrlMYAXVQe29fPU.A1qzB0q34e_9tysLnbHKvnXAj49583OrPNUL7cPbC5Q

    ACMEClient::instance().dummyCallback(domain_name, url, key);
}

}


ACMEClient & ACMEClient::instance()
{
    static ACMEClient instance;
    return instance;
}

void ACMEClient::reload(const Poco::Util::AbstractConfiguration &)
try
{
    if (!client)
    {
        acme_lw::AcmeClient::init(acme_lw::AcmeClient::Environment::STAGING);

        auto cert_buf = ReadBufferFromFile("/home/thevar1able/src/clickhouse/cmake-build-debug/acme/acme.key");
        std::string cert_pem;
        readStringUntilEOF(cert_pem, cert_buf);
        LOG_DEBUG(log, "Read certificate from file: {}", cert_pem);

        client = std::make_unique<acme_lw::AcmeClient>(cert_pem);
    }

    auto context = Context::getGlobalContextInstance();
    auto zk = context->getZooKeeper();

    zk->createIfNotExists(fs::path(ZOOKEEPER_ACME_BASE_PATH), "");

    BackgroundSchedulePool & bgpool = context->getSchedulePool();

    election_task = bgpool.createTask("ACMEClient", [this, zk] {
        LOG_DEBUG(log, "Running election task");

        election_task->scheduleAfter(1000);

        auto leader = zkutil::EphemeralNodeHolder::tryCreate(fs::path(ZOOKEEPER_ACME_BASE_PATH) / "leader", *zk);
        if (leader)
            leader_node = std::move(leader);

        LOG_DEBUG(log, "I'm the leader: {}", leader_node ? "yes" : "no");
    });
    election_task->activateAndSchedule();

    refresh_task = bgpool.createTask("ACMEClient", [this] {
        LOG_DEBUG(log, "Running ACMEClient task");

        refresh_task->scheduleAfter(1000);
    });

    refresh_task->activateAndSchedule();

    initialized = true;

    // domains = {"thevar1able.com"};
    // requestChallenge("http://localhost:8080/.well-known/acme-challenge/1234");

    auto nonce = requestNonce();
    LOG_DEBUG(log, "Getting nonce: {}", nonce);

    auto cert = Poco::Crypto::RSAKey("", "/home/thevar1able/src/clickhouse/cmake-build-debug/acme/acme.key", "");

    authenticate(cert);
}
catch (...)
{
    tryLogCurrentException("Failed :(");
}


/*
* get directory -- GET https://acme-staging-v02.api.letsencrypt.org/directory
* get nonce -- HEAD https://acme-staging-v02.api.letsencrypt.org/acme/new-nonce -- take replay-nonce header
*
*
*/

void ACMEClient::getDirectory()
{
    auto uri = Poco::URI("https://acme-staging-v02.api.letsencrypt.org/directory");
    auto r = Poco::Net::HTTPRequest("GET", uri.getPathAndQuery());

    auto timeout = ConnectionTimeouts();
    auto proxy = ProxyConfiguration();
    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeout, proxy);

    session->sendRequest(r);

    auto response = Poco::Net::HTTPResponse();
    session->receiveResponse(response);

    std::cout << "Response: " << response.getStatus() << std::endl;
}

void ACMEClient::authenticate(Poco::Crypto::RSAKey & key)
{
    const auto nonce = requestNonce();

    // {
    //   "protected": base64url({
    //     "alg": "RS256",
    //     "jwk": {...},
    //     "nonce": "6S8IqOGY7eL2lsGoTZYifg",
    //     "url": "https://example.com/acme/new-account"
    //   }),
    //   "payload": base64url({
    //     "termsOfServiceAgreed": true,
    //     "contact": [
    //       "mailto:cert-admin@example.org",
    //       "mailto:admin@example.org"
    //     ]
    //   }),
    //   "signature": "RZPOnYoPs1PhjszF...-nh6X1qtOFPB519I"
    // }

    auto e = key.encryptionExponent();
    auto n = key.modulus();

    auto e_str = std::string(e.begin(), e.end());
    auto n_str = std::string(n.begin(), n.end());

    auto e_enc = base64Encode(e_str, /* url_encoding */ true);
    auto n_enc = base64Encode(n_str, /* url_encoding */ true);

    trimRight(e_enc, '=');
    trimRight(n_enc, '=');

    std::string jwk = fmt::format(R"({{"e":"{}","kty":"RSA","n":"{}"}})", e_enc, n_enc);
    LOG_DEBUG(log, "JWK: {}", jwk);

    auto protected_data = fmt::format(R"({{"alg":"RS256","jwk":{},"nonce":"{}","url":"https://acme-staging-v02.api.letsencrypt.org/acme/new-acct"}})", jwk, nonce);
    LOG_DEBUG(log, "Protected data: {}", protected_data);

    auto protected_enc = base64Encode(protected_data, /* url_encoding */ true);
    LOG_DEBUG(log, "Protected data encoded: {}", protected_enc);

    std::string payload = R"({"termsOfServiceAgreed":true,"contact":["mailto:admin@example.org"]})";
    auto payload_enc = base64Encode(payload, /* url_encoding */ true);
    LOG_DEBUG(log, "Payload: {}", payload);

    trimRight(protected_enc, '=');
    trimRight(payload_enc, '=');
    std::string to_sign = protected_enc + "." + payload_enc;

    EVP_PKEY * pkey = EVP_PKEY_new();
    auto ret = EVP_PKEY_assign_RSA(pkey, key.impl()->getRSA());
    if (ret != 1)
    {
        throw std::runtime_error("Error assigning RSA key to EVP_PKEY");
    }

    size_t signature_length = 0;

    EVP_MD_CTX * context(EVP_MD_CTX_create());
    const EVP_MD * sha256 = EVP_get_digestbyname("SHA256");
    if (!sha256 ||
        EVP_DigestInit_ex(context, sha256, nullptr) != 1 ||
        EVP_DigestSignInit(context, nullptr, sha256, nullptr, pkey) != 1 ||
        EVP_DigestSignUpdate(context, to_sign.c_str(), to_sign.size()) != 1 ||
        EVP_DigestSignFinal(context, nullptr, &signature_length) != 1)
    {
        throw std::runtime_error("Error creating SHA256 digest");
    }

    std::vector<unsigned char> signature(signature_length);
    if (EVP_DigestSignFinal(context, &signature.front(), &signature_length) != 1)
    {
        throw std::runtime_error("Error creating SHA256 digest in final signature");
    }

    std::string signature_str = std::string(signature.begin(), signature.end());
    signature_str = base64Encode(signature_str, /* url_encoding */ true);

    trimRight(signature_str, '=');

    std::string request_data = R"({"protected":")" + protected_enc + R"(","payload":")" + payload_enc + R"(","signature":")" + signature_str + R"("})";
    LOG_DEBUG(log, "Request data: {}", request_data);

    auto uri = Poco::URI("https://acme-staging-v02.api.letsencrypt.org/acme/new-acct");
    auto r = Poco::Net::HTTPRequest(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery());
    r.set("Content-Type", "application/jose+json");
    r.set("Content-Length", std::to_string(request_data.size()));

    auto timeout = ConnectionTimeouts();
    auto proxy = ProxyConfiguration();
    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeout, proxy);

    auto &ostream = session->sendRequest(r);
    ostream << request_data;

    auto response = Poco::Net::HTTPResponse();
    auto &rstream = session->receiveResponse(response);
    std::cout << rstream.rdbuf() << std::endl;

    for (const auto & header : response)
    {
        std::cout << header.first << ": " << header.second << std::endl;
    }

    std::cout << "Response: " << response.getStatus() << std::endl;
}

std::string ACMEClient::requestNonce()
{
    auto uri = Poco::URI("https://acme-staging-v02.api.letsencrypt.org/acme/new-nonce");
    auto r = Poco::Net::HTTPRequest("HEAD", uri.getPathAndQuery());
    LOG_DEBUG(log, "Requesting nonce from {}", uri.getPathAndQuery());

    auto timeout = ConnectionTimeouts();
    auto proxy = ProxyConfiguration();
    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeout, proxy);

    session->sendRequest(r);

    auto response = Poco::Net::HTTPResponse();
    session->receiveResponse(response);

    std::cout << "Response: " << response.getStatus() << std::endl;

    auto nonce = response.get("replay-nonce");
    return nonce;
}

void ACMEClient::dummyCallback(const std::string & domain_name, const std::string & url, const std::string & key)
{
    LOG_DEBUG(log, "Callback for domain {} with url {} and key {}", domain_name, url, key);
}

std::string ACMEClient::requestChallenge(const std::string & uri)
{
    LOG_DEBUG(log, "Requesting challenge for {}", uri);

    client->issueCertificate({domains.begin(), domains.end()}, dumberCallback);

    return "";
}

}
}
