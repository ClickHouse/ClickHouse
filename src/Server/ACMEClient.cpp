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
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/SHA1Engine.h>
#include <Poco/String.h>
#include <Poco/URI.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>


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

}


ACMEClient & ACMEClient::instance()
{
    static ACMEClient instance;
    return instance;
}

void ACMEClient::requestCertificate(const Poco::Util::AbstractConfiguration & )
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
    authenticate(key);

    order(key);

    /// TODO on initialization go through config and try to reissue all
    /// expiring/missing certificates.

    initialized = true;
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

void ACMEClient::authenticate(Poco::Crypto::RSAKey & key)
{
    const auto nonce = requestNonce();

    /// FROM RFC 8555
    /// {
    ///   "protected": base64url({
    ///     "alg": "RS256",
    ///     "jwk": {...},
    ///     "nonce": "6S8IqOGY7eL2lsGoTZYifg",
    ///     "url": "https://example.com/acme/new-account"
    ///   }),
    ///   "payload": base64url({
    ///     "termsOfServiceAgreed": true,
    ///     "contact": [
    ///       "mailto:cert-admin@example.org",
    ///       "mailto:admin@example.org"
    ///     ]
    ///   }),
    ///   "signature": "RZPOnYoPs1PhjszF...-nh6X1qtOFPB519I"
    /// }

    auto uri = Poco::URI("https://acme-staging-v02.api.letsencrypt.org/acme/new-acct");

    auto jwk = JSONWebKey::fromRSAKey(key).toString();
    auto protected_enc = formProtectedData(jwk, nonce, uri.toString(), "");

    std::string payload = R"({"termsOfServiceAgreed":true,"contact":["mailto:admin@example.org"]})";
    auto payload_enc = base64Encode(payload, /*url_encoding*/ true, /*no_padding*/ true);

    std::string to_sign = protected_enc + "." + payload_enc;
    auto signature = hmacSha256(to_sign, key);

    std::string request_data
        = R"({"protected":")" + protected_enc + R"(","payload":")" + payload_enc + R"(","signature":")" + signature + R"("})";
    LOG_DEBUG(log, "Request data: {}", request_data);

    auto r = Poco::Net::HTTPRequest(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery());
    r.set("Content-Type", "application/jose+json");
    r.set("Content-Length", std::to_string(request_data.size()));

    auto timeout = ConnectionTimeouts();
    auto proxy = ProxyConfiguration();
    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeout, proxy);

    auto & ostream = session->sendRequest(r);
    ostream << request_data;

    auto response = Poco::Net::HTTPResponse();
    auto & rstream = session->receiveResponse(response);

    /// checking for HTTP code would be nice here

    Poco::JSON::Parser parser;
    auto json = parser.parse(rstream).extract<Poco::JSON::Object::Ptr>();

    //     HTTP/1.1 201 Created
    // Content-Type: application/json
    // Replay-Nonce: D8s4D2mLs8Vn-goWuPQeKA
    // Link: <https://example.com/acme/directory>;rel="index"
    // Location: https://example.com/acme/acct/evOfKhNU60wg
    //
    // {
    //   "status": "valid",
    //
    //   "contact": [
    //     "mailto:cert-admin@example.org",
    //     "mailto:admin@example.org"
    //   ],
    //
    //   "orders": "https://example.com/acme/acct/evOfKhNU60wg/orders"
    // }


    key_id = response.get("Location");
    for (const auto & [k, v] : response)
    {
        LOG_DEBUG(log, "Response header: {} -> {}", k, v);
    }

    // json->getValue<std::string>(Directory::new_nonce_key),
}

void ACMEClient::order(Poco::Crypto::RSAKey & key)
{
    auto uri = Poco::URI(directory->new_order);
    auto r = Poco::Net::HTTPRequest("POST", uri.getPathAndQuery());
    r.set("Content-Type", "application/jose+json");

    auto timeout = ConnectionTimeouts();
    auto proxy = ProxyConfiguration();
    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, timeout, proxy);

    // POST /acme/new-order HTTP/1.1
    // Host: example.com
    // Content-Type: application/jose+json
    //
    // {
    //   "protected": base64url({
    //     "alg": "ES256",
    //     "kid": "https://example.com/acme/acct/evOfKhNU60wg",
    //     "nonce": "5XJ1L3lEkMG7tR6pA00clA",
    //     "url": "https://example.com/acme/new-order"
    //   }),
    //   "payload": base64url({
    //     "identifiers": [
    //       { "type": "dns", "value": "www.example.org" },
    //       { "type": "dns", "value": "example.org" }
    //     ]
    //   }),
    //   "signature": "H6ZXtGjTZyUnPeKn...wEA4TklBdh3e454g"
    // }

    auto nonce = requestNonce();
    auto protected_data = formProtectedData("", nonce, uri.toString(), key_id);

    std::string payload = R"({"identifiers":[{"type":"dns","value":"example.com"}]})";
    LOG_DEBUG(log, "Payload: {}", payload);
    auto payload_enc = base64Encode(payload, /*url_encoding*/ true, /*no_padding*/ true);

    std::string to_sign = protected_data + "." + payload_enc;
    auto signature = hmacSha256(to_sign, key);

    std::string request_data = R"({"protected":")" + protected_data + R"(","payload":")" + payload_enc + R"(","signature":")" + signature + R"("})";
    r.set("Content-Length", std::to_string(request_data.size()));

    auto &ostream = session->sendRequest(r);
    ostream << request_data;

    auto response = Poco::Net::HTTPResponse();
    auto &rstream = session->receiveResponse(response);

    // Poco::JSON::Parser parser;
    // auto json = parser.parse(rstream).extract<Poco::JSON::Object::Ptr>();

    std::cout << "Response: " << rstream.rdbuf() << std::endl;
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
    session->receiveResponse(response);

    // std::cout << "Response: " << response.getStatus() << std::endl;

    auto nonce = response.get("replay-nonce");
    return nonce;
}

// void ACMEClient::dummyCallback(const std::string & domain_name, const std::string & url, const std::string & key)
// {
//     LOG_DEBUG(log, "Callback for domain {} with url {} and key {}", domain_name, url, key);
// }

std::string ACMEClient::requestChallenge(const std::string & uri)
{
    /// TODO go to Keeper and query existing challenge for domain
    /// if not present, return "".

    LOG_DEBUG(log, "Requesting challenge for {}", uri);

    // client->issueCertificate({domains.begin(), domains.end()}, dumberCallback);

    return "";
}

}
}
