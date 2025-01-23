#include <Server/ACMEClient.h>
#include "IO/ReadWriteBufferFromHTTP.h"

#if USE_SSL

#include <memory>
#include <mutex>
#include <sstream>

#include <fmt/core.h>

#include <openssl/bio.h>
#include <openssl/core_names.h>
#include <openssl/encoder.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/obj_mac.h>
#include <openssl/pem.h>
#include <openssl/provider.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Disks/IO/ReadBufferFromWebServer.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/Credentials.h>
#include <Interpreters/Context.h>
#include <Poco/Base64Encoder.h>
#include <Poco/Crypto/CryptoStream.h>
#include <Poco/Crypto/ECKey.h>
#include <Poco/Crypto/RSADigestEngine.h>
#include <Poco/Crypto/RSAKey.h>
#include <Poco/Crypto/RSAKeyImpl.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Poco/DateTimeFormat.h>
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
#include <Poco/Timespan.h>
#include <Poco/URI.h>
#include <Server/CertificateReloader.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int OPENSSL_ERROR;
}

namespace ACMEClient
{

namespace
{

namespace fs = std::filesystem;

std::string generateCSR(std::string pkey, std::vector<std::string> domain_names)
{
    if (domain_names.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No domain names provided");

    auto name = domain_names.front();

    EVP_PKEY * key = EVP_PKEY_new();
    BIO * pkey_bio(BIO_new_mem_buf(pkey.c_str(), -1));
    if (PEM_read_bio_PrivateKey(pkey_bio, &key, nullptr, nullptr) == nullptr)
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error reading private key: {}", getOpenSSLErrors());
    }

    X509_REQ * req(X509_REQ_new());
    X509_NAME * cn = X509_REQ_get_subject_name(req);
    if (!X509_NAME_add_entry_by_txt(cn, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char *>(name.c_str()), -1, -1, 0))
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error adding CN to X509_NAME");
    }

    if (domain_names.size() > 1)
    {
        X509_EXTENSIONS * extensions(sk_X509_EXTENSION_new_null());

        std::string other_domain_names;
        for (auto it = domain_names.begin() + 1; it != domain_names.end(); ++it)
        {
            if (it->empty())
                continue;
            other_domain_names += fmt::format("DNS:{}, ", *it);
        }

        auto * nid = X509V3_EXT_conf_nid(nullptr, nullptr, NID_subject_alt_name, other_domain_names.c_str());
        // ossl_check_X509_EXTENSION_type(nid);

        int ret = OPENSSL_sk_push(reinterpret_cast<OPENSSL_STACK *>(extensions), static_cast<const void *>(nid));
        if (!ret)
        {
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Unable to add Subject Alternative Name to extensions {}", getOpenSSLErrors());
        }

        if (X509_REQ_add_extensions(req, extensions) != 1)
        {
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Unable to add Subject Alternative Names to CSR {}", getOpenSSLErrors());
        }
    }

    if (!X509_REQ_set_pubkey(req, key))
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failure in X509_REQ_set_pubkey");
    }

    if (!X509_REQ_sign(req, key, EVP_sha256()))
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failure in X509_REQ_sign");
    }

    BIO * req_bio(BIO_new(BIO_s_mem()));
    if (i2d_X509_REQ_bio(req_bio, req) < 0)
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failure in i2d_X509_REQ_bio");
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

    LOG_DEBUG(&Poco::Logger::get("ACME"), "CSR: {}", csr);

    return csr;
}

std::string generatePrivateKeyInPEM()
{
    auto key = Poco::Crypto::RSAKey(Poco::Crypto::RSAKey::KL_4096, Poco::Crypto::RSAKey::EXP_LARGE);

    BIO * key_bio(BIO_new(BIO_s_mem()));
    if (PEM_write_bio_RSAPrivateKey(key_bio, key.impl()->getRSA(), nullptr, nullptr, 0, nullptr, nullptr) != 1)
    {
        BIO_free(key_bio);
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error writing private key to BIO: {}", getOpenSSLErrors());
    }

    char * data;
    size_t data_len = BIO_get_mem_data(key_bio, &data);

    std::string private_key(data, data_len);
    BIO_free(key_bio);

    return private_key;
}

}

using DirectoryPtr = std::shared_ptr<Directory>;

ACMEClient & ACMEClient::instance()
{
    static ACMEClient instance;
    return instance;
}

std::optional<std::tuple<Poco::Crypto::EVPPKey, Poco::Crypto::X509Certificate>> ACMEClient::requestCertificate(const Poco::Util::AbstractConfiguration &)
{
    LOG_TEST(log, "ACME requestCertificate called");

    auto context = Context::getGlobalContextInstance();
    auto zk = context->getZooKeeper();

    std::string pkey;
    std::string certificate;

    zk->tryGet(fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "domains" / domains[0] / "private_key", pkey);
    zk->tryGet(fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "domains" / domains[0] / "certificate", certificate);

    if (pkey.empty() || certificate.empty())
    {
        LOG_DEBUG(log, "No certificate found for domain {}", domains[0]);
        return {};
    }

    std::istringstream pkey_stream(pkey);  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    std::istringstream certificate_stream(certificate);  // STYLE_CHECK_ALLOW_STD_STRING_STREAM

    return std::make_tuple(Poco::Crypto::EVPPKey(nullptr, &pkey_stream), Poco::Crypto::X509Certificate(certificate_stream));
}

/// uninitialized -> load/generate key -> authorize -> watch for expiration
void ACMEClient::initialize(const Poco::Util::AbstractConfiguration & config)
{
    if (initialized)
        return;

    if (private_acme_key && !key_id.empty())
        return;

    // auto http_port = config.getInt("http_port");
    // if (http_port != 80 && !initialized)
    //     LOG_WARNING(log, "For ACME HTTP challenge HTTP port must be 80, but is {}", http_port);

    if (!config.has("acme"))
        return;

    directory_url = config.getString("acme.directory_url", LetsEncrypt::ACME_STAGING_DIRECTORY_URL);  /// FIXME
    contact_email = config.getString("acme.email", "");
    terms_of_service_agreed = config.getBool("acme.terms_of_service_agreed");

    Poco::Util::AbstractConfiguration::Keys domains_keys;
    config.keys("acme.domains", domains_keys);

    std::vector<std::string> served_domains;
    for (const auto & key : domains_keys)
    {
        served_domains.push_back(config.getString("acme.domains." + key));
        LOG_DEBUG(log, "Serving domain: {}", served_domains.back());
    }
    domains = served_domains;
    refresh_certificates_interval = config.getInt("acme.refresh_certificates_interval", 1000 * 60 * 60);

    connection_timeout_settings = ConnectionTimeouts();
    proxy_configuration = ProxyConfiguration();

    acme_hostname = Poco::URI(directory_url).getHost();
    LOG_DEBUG(log, "ACME hostname: {}", acme_hostname);

    zookeeper = Context::getGlobalContextInstance()->getZooKeeper();

    zookeeper->createIfNotExists(fs::path(ZOOKEEPER_ACME_BASE_PATH), "");
    zookeeper->createIfNotExists(fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname, "");
    zookeeper->createIfNotExists(fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "challenges", "");
    zookeeper->createIfNotExists(fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "domains", "");

    for (const auto & domain : domains)
        zookeeper->createIfNotExists(fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "domains" / domain, "");

    BackgroundSchedulePool & bgpool = Context::getGlobalContextInstance()->getSchedulePool();

    refresh_certificates_task = bgpool.createTask("ACMECertRefresh", [this, &config] {refresh_certificates_fn(config);});
    authentication_task = bgpool.createTask("ACMEAuth", [this] { authentication_fn(); });
    refresh_key_task = bgpool.createTask("ACMEKeygen", [this] { refresh_key_fn(); });

    if (!private_acme_key)
        refresh_key_task->activateAndSchedule();
}

void ACMEClient::refresh_certificates_fn(const Poco::Util::AbstractConfiguration & config)
{
    try
    {
        auto context = Context::getGlobalContextInstance();
        auto zk = context->getZooKeeper();

        bool need_refresh = false;
        for (const auto & domain : domains)
        {
            Coordination::Stat certificate_stat;
            Coordination::Stat private_key_stat;

            std::string private_key;
            std::string certificate;

            zookeeper->tryGet(fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "domains" / domain / "private_key", private_key, &private_key_stat);
            zookeeper->tryGet(fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "domains" / domain / "certificate", certificate, &certificate_stat);

            if (private_key.empty() || certificate.empty())
            {
                need_refresh = true;
                break;
            }

            std::istringstream cert_stream(certificate); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            auto wcert = Poco::Crypto::X509Certificate(cert_stream);

            LOG_DEBUG(log, "Certificate for domain {} expires on {}", domain, Poco::DateTimeFormatter::format(wcert.expiresOn(), Poco::DateTimeFormat::ISO8601_FORMAT));

            /// TODO configurable
            if (wcert.expiresOn() < Poco::Timestamp() + Poco::Timespan(60 * Poco::Timespan::DAYS))
                need_refresh = true;
        }

        if (!need_refresh)
        {
            LOG_DEBUG(log, "No certificates need to be refreshed");

            CertificateReloader::instance().tryLoad(config);

            refresh_certificates_task->scheduleAfter(60000);
            return;
        }

        auto active_order_path = fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "active_order";

        if ((!lock || !lock->isLocked()) && zk->exists(active_order_path))
        {
            LOG_DEBUG(log, "Certificate order is in progress by another replica, skipping");
            refresh_certificates_task->scheduleAfter(60000);
            return;
        }

        if (!active_order.has_value())
        {
            lock = std::make_shared<zkutil::ZooKeeperLock>(zk, fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname, "active_order", "ACMEClient");
            lock->tryLock();

            active_order = order();

            refresh_certificates_task->scheduleAfter(1000);
            return;
        }

        lock->tryLock();

        auto order_url = active_order.value();
        auto order_data = describeOrder(order_url);
        LOG_DEBUG(log, "Order {} status: {}", order_url, order_data.status);

        if (order_data.status == "invalid")
        {
            active_order.reset();
            lock.reset();

            refresh_certificates_task->scheduleAfter(10000);
            return;
        }

        if (order_data.status == "ready")
        {
            auto pkey = generatePrivateKeyInPEM();

            for (const auto & domain : domains)
            {
                auto path = fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "domains" / domain / "private_key";
                zk->createOrUpdate(path, pkey, zkutil::CreateMode::Persistent);
                LOG_DEBUG(log, "Updated private key for domain {}", domain);
            }

            LOG_DEBUG(log, "Finalizing order {}", order_url);
            finalizeOrder(order_data.finalize_url, pkey);

            refresh_certificates_task->scheduleAfter(5000);
            return;
        }

        if (order_data.status == "valid")
        {
            auto certificate = pullCertificate(order_data.certificate_url);
            for (const auto & domain : domains)
            {
                auto path = fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "domains" / domain / "certificate";
                zk->createOrUpdate(path, certificate, zkutil::CreateMode::Persistent);
                LOG_DEBUG(log, "Updated certificate for domain {}", domain);
            }

            active_order.reset();
            lock.reset();

            LOG_DEBUG(log, "Certificate refresh finished successfully");
        }

        /// Handle unknown status

        CertificateReloader::instance().tryLoad(config);
        LOG_DEBUG(log, "Reloaded active certificates");
    }
    catch (...)
    {
        refresh_certificates_task->scheduleAfter(10000);
        tryLogCurrentException("ACMEClient");
    }

    refresh_certificates_task->scheduleAfter(60000); /// fixme config
}

void ACMEClient::authentication_fn()
{
    LOG_DEBUG(log, "Running ACMEClient authentication task");
    if (!key_id.empty())
        return;

    if (!private_acme_key)
    {
        LOG_WARNING(log, "Private key is not initialized, retrying in 1 second. This is a bug.");
        refresh_key_task->scheduleAfter(1000);
        return;
    }

    try
    {
        if (!directory)
            directory = getDirectory();

        authenticate();

        refresh_certificates_task->activateAndSchedule();
        authentication_task->deactivate();
    }
    catch (...)
    {
        authentication_task->scheduleAfter(1000);
        tryLogCurrentException("ACMEClient");
    }
}

void ACMEClient::refresh_key_fn()
{
    LOG_DEBUG(log, "Running ACMEClient key refresh task");

    if (private_acme_key)
        return;

    auto context = Context::getGlobalContextInstance();
    auto zk = context->getZooKeeper();

    Coordination::Stat private_key_stat;
    std::string private_key;

    zk->tryGet(fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "account_private_key", private_key, &private_key_stat);
    if (private_key.empty())
    {
        LOG_DEBUG(log, "Generating new RSA private key for ACME account");
        private_key = generatePrivateKeyInPEM();

        try
        {
            /// TODO handle exception
            zk->createIfNotExists(fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "account_private_key", private_key);

            LOG_DEBUG(log, "Private key saved to ZooKeeper");
        }
        catch (...)
        {
            refresh_key_task->scheduleAfter(1000);
            tryLogCurrentException("ACMEClient");
        }

        refresh_key_task->schedule();
        return;
    }

    chassert(!private_key.empty());
    chassert(!private_acme_key);

    {
        std::lock_guard key_lock(private_acme_key_mutex);
        std::istringstream private_key_stream(private_key); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        private_acme_key = std::make_shared<Poco::Crypto::RSAKey>(nullptr, &private_key_stream, "");
    }

    LOG_DEBUG(log, "ACME private key successfully loaded");

    authentication_task->activateAndSchedule();
    refresh_key_task->deactivate();

    keys_initialized = true;
}


DirectoryPtr ACMEClient::getDirectory()
{
    LOG_DEBUG(log, "Requesting ACME directory from {}", directory_url);

    std::string buf;
    ReadSettings read_settings;
    auto reader = std::make_unique<ReadBufferFromWebServer>(
        directory_url,
        Context::getGlobalContextInstance(),
        DBMS_DEFAULT_BUFFER_SIZE,
        read_settings,
        /* use_external_buffer */ true,
        /* read_until_position */ 0);
    readStringUntilEOF(buf, *reader);

    return std::make_shared<Directory>(Directory::parse(buf));
}

std::string
ACMEClient::doJWSRequest(const std::string & url, const std::string & payload, std::shared_ptr<Poco::Net::HTTPResponse> response)
{
    if (!private_acme_key)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Account private key is not initialized");

    std::string nonce;
    /// FIXME
    for (int i = 0; i < 10; ++i)
    {
        try
        {
            nonce = requestNonce();
            break;
        }
        catch (...)
        {
            tryLogCurrentException("ACMEClient");
        }
    }

    LOG_DEBUG(log, "Making JWS request to URL: {}", url);

    auto uri = Poco::URI(url);

    std::string protected_enc;
    {
        std::string protected_data;

        if (!key_id.empty())
            protected_data = fmt::format(R"({{"alg":"RS256","kid":"{}","nonce":"{}","url":"{}"}})", key_id, nonce, url);
        else
        {
            std::lock_guard key_lock(private_acme_key_mutex);
            auto jwk = JSONWebKey::fromRSAKey(*private_acme_key).toString();
            protected_data = fmt::format(R"({{"alg":"RS256","jwk":{},"nonce":"{}","url":"{}"}})", jwk, nonce, url);
        }

        LOG_DEBUG(log, "Protected data: {}", protected_data);

        protected_enc = base64Encode(protected_data, /*url_encoding*/ true, /*no_padding*/ true);
    }

    LOG_DEBUG(log, "Payload: {}", payload);
    auto payload_enc = base64Encode(payload, /*url_encoding*/ true, /*no_padding*/ true);

    std::string to_sign = protected_enc + "." + payload_enc;
    std::string signature;
    {
        std::lock_guard key_lock(private_acme_key_mutex);
        signature = calculateHMACwithSHA256(to_sign, *private_acme_key);
    }

    std::string request_data
        = R"({"protected":")" + protected_enc + R"(","payload":")" + payload_enc + R"(","signature":")" + signature + R"("})";


    auto r = Poco::Net::HTTPRequest(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery());
    r.set("Content-Type", "application/jose+json");
    r.set("Content-Length", std::to_string(request_data.size()));

    LOG_DEBUG(log, "Requesting {} with payload: {}", url, request_data);
    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, connection_timeout_settings, proxy_configuration);

    auto & ostream = session->sendRequest(r);
    ostream << request_data;

    if (!response)
        response = std::make_shared<Poco::Net::HTTPResponse>();

    auto * rstream = receiveResponse(*session, r, *response, /* allow_redirects */ false);

    std::string response_str;
    Poco::StreamCopier::copyToString(*rstream, response_str);

    return response_str;
}

void ACMEClient::authenticate()
{
    if (!key_id.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Already authenticated");

    Poco::JSON::Object payload_object;
    auto contact = Poco::JSON::Array();

    if (!contact_email.empty())
        contact.add("mailto:" + contact_email);

    payload_object.set("contact", contact);
    payload_object.set("termsOfServiceAgreed", terms_of_service_agreed);

    std::ostringstream payload; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    payload.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(payload_object, payload);

    LOG_TEST(log, "Account payload: {}", payload.str());

    auto http_response = std::make_shared<Poco::Net::HTTPResponse>();
    auto response = doJWSRequest(directory->new_account, payload.str(), http_response);

    Poco::JSON::Parser parser;
    auto json = parser.parse(response).extract<Poco::JSON::Object::Ptr>();

    LOG_TEST(log, "Account response: {}", response);

    if (!json->has("status") || json->getValue<std::string>("status") != "valid")
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Account creation failed");

    key_id = (*http_response).get("Location");
    for (const auto & [k, v] : *http_response)
        LOG_TEST(log, "Response header: {} -> {}", k, v);
}

std::string ACMEClient::order()
{
    Poco::JSON::Object payload_object;
    auto payload_identifiers = Poco::JSON::Array();

    for (const auto & domain : domains)
    {
        Poco::JSON::Object identifier;
        identifier.set("type", "dns");
        identifier.set("value", domain);
        payload_identifiers.add(identifier);
    }

    payload_object.set("identifiers", payload_identifiers);

    std::ostringstream payload; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    payload.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(payload_object, payload);

    auto http_response = std::make_shared<Poco::Net::HTTPResponse>();
    auto response = doJWSRequest(directory->new_order, payload.str(), http_response);

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

    for (const auto & auth : *authorizations)
    {
        LOG_DEBUG(log, "Authorization: {}", auth.toString());

        processAuthorization(auth.toString());
    }

    return order_url;
}


ACMEOrder ACMEClient::describeOrder(const std::string & order_url)
{
    std::string read_buffer;
    ReadSettings read_settings;
    auto reader = std::make_unique<ReadBufferFromWebServer>(
        order_url,
        Context::getGlobalContextInstance(),
        DBMS_DEFAULT_BUFFER_SIZE,
        read_settings,
        /* use_external_buffer */ true,
        /* read_until_position */ 0);
    readStringUntilEOF(read_buffer, *reader);

    Poco::JSON::Parser parser;
    auto json = parser.parse(read_buffer).extract<Poco::JSON::Object::Ptr>();

    LOG_DEBUG(log, "DescribeOrder response: {}", read_buffer);

    auto status = json->getValue<std::string>("status");
    /// TODO check expiration
    // auto expires = json->getValue<std::string>("expires");

    auto identifiers = json->getArray("identifiers");
    auto authorizations = json->getArray("authorizations");

    auto finalize = json->getValue<std::string>("finalize");

    std::string certificate;
    if (json->has("certificate"))
        certificate = json->getValue<std::string>("certificate");

    auto order_data = ACMEOrder{
        .status = status,
        .order_url = order_url,
        .finalize_url = finalize,
        .certificate_url = certificate,
    };

    return order_data;
}

std::string ACMEClient::pullCertificate(const std::string & certificate_url)
{
    std::string certificate;
    ReadSettings read_settings;
    auto reader = std::make_unique<ReadBufferFromWebServer>(
        certificate_url,
        Context::getGlobalContextInstance(),
        DBMS_DEFAULT_BUFFER_SIZE,
        read_settings,
        /* use_external_buffer */ true,
        /* read_until_position */ 0);
    readStringUntilEOF(certificate, *reader);

    LOG_TEST(log, "PullCertificate response: {}", certificate);

    return certificate;
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
        const auto & ch = challenge.extract<Poco::JSON::Object::Ptr>();

        if (ch->has("validated"))
        {
            LOG_DEBUG(log, "Challenge is already validated");
            continue;
        }

        auto type = ch->getValue<std::string>("type");
        auto url = ch->getValue<std::string>("url");
        auto status = ch->getValue<std::string>("status");
        auto token = ch->getValue<std::string>("token");

        LOG_DEBUG(log, "Challenge: type: {}, url: {}, status: {}, token: {}", type, url, status, token);
        if (type == HTTP_01_CHALLENGE_TYPE)
        {
            if (status == "valid")
            {
                LOG_TEST(log, "Challenge is already validated");
                continue;
            }

            /// TODO identifier.value as a hostname
            auto zk = Context::getGlobalContextInstance()->getZooKeeper();
            zk->createIfNotExists(fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "challenges" / token, token);

            auto response = doJWSRequest(url, "{}", nullptr);

            LOG_TEST(log, "Response: {}", response);
        }
    }
}

void ACMEClient::finalizeOrder(const std::string & finalize_url, const std::string & pkey)
{
    auto context = Context::getGlobalContextInstance();
    auto zk = context->getZooKeeper();

    std::string csr = generateCSR(pkey, domains);
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

    LOG_TEST(log, "Requesting nonce from {}", directory->new_nonce);

    auto uri = Poco::URI(directory->new_nonce);
    auto r = Poco::Net::HTTPRequest(Poco::Net::HTTPRequest::HTTP_HEAD, uri.getPathAndQuery());

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, uri, connection_timeout_settings, proxy_configuration);
    session->sendRequest(r);

    auto response = Poco::Net::HTTPResponse();
    receiveResponse(*session, r, response, /* allow_redirects */ false);

    return response.get(NONCE_HEADER_NAME);
}

std::string ACMEClient::requestChallenge(const std::string & uri)
{
    LOG_DEBUG(log, "Challenge requested for uri: {}", uri);

    if (!uri.starts_with(ACME_CHALLENGE_HTTP_PATH))
        return "";

    std::vector<std::string> uri_segments;
    Poco::URI parsed_url(uri);
    parsed_url.getPathSegments(uri_segments);

    /// We've already validated prefix, segments[2] should be our token.
    if (uri_segments.size() < 3)
        return "";
    std::string token_from_uri = uri_segments[2];

    auto context = Context::getGlobalContextInstance();
    auto zk = context->getZooKeeper();

    Coordination::Stat challenge_stat;
    std::string challenge;

    auto response = zk->tryGet(
        fs::path(ZOOKEEPER_ACME_BASE_PATH) / acme_hostname / "challenges" / token_from_uri,
        challenge,
        &challenge_stat
    );
    if (!response)
        return "";

    if (challenge.empty())
    {
        LOG_DEBUG(log, "No challenge found for uri: {}", uri);
        return "";
    }

    if (challenge != token_from_uri)
    {
        LOG_DEBUG(log, "Challenge mismatch for uri: {}, expected: {}, got: {}", uri, token_from_uri, challenge);
        return "";
    }

    auto jwk = JSONWebKey::fromRSAKey(*private_acme_key).toString();
    auto jwk_thumbprint = encodeSHA256(jwk);
    jwk_thumbprint = base64Encode(jwk_thumbprint, /*url_encoding*/ true, /*no_padding*/ true);

    refresh_certificates_task->scheduleAfter(1000);
    return token_from_uri + "." + jwk_thumbprint;
}

}
}
#endif
