#include <memory>
#include <sstream>
#include <Server/ACMEClient.h>

#if USE_SSL
#include <Core/BackgroundSchedulePool.h>
#include <Disks/IO/ReadBufferFromWebServer.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/Credentials.h>
#include <Interpreters/Context.h>
#include <fmt/core.h>
#include <openssl/core_names.h>
#include <openssl/encoder.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/obj_mac.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/provider.h>
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
#include <Common/OpenSSLHelpers.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>


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

// void dumberCallback(const std::string & domain_name, const std::string & url, const std::string & key)
// {
//     /// Callback for domain thevar1able.com with url
//     /// http://thevar1able.com/.well-known/acme-challenge/TU9yW7Ad6RgzrmILfp8Zyn9swtxhYrlMYAXVQe29fPU
//     /// and key TU9yW7Ad6RgzrmILfp8Zyn9swtxhYrlMYAXVQe29fPU.A1qzB0q34e_9tysLnbHKvnXAj49583OrPNUL7cPbC5Q
//
//     ACMEClient::instance().dummyCallback(domain_name, url, key);
// }
std::string generateCSR(std::vector<std::string> domain_names)
{
    if (domain_names.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No domain names provided");

    auto name = domain_names.front();

    auto bits = 4096;
    EVP_PKEY * key(EVP_RSA_gen(bits));

    X509_REQ * req(X509_REQ_new());
    X509_NAME * cn = X509_REQ_get_subject_name(req);
    if (!X509_NAME_add_entry_by_txt(cn, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char *>(name.c_str()), -1, -1, 0))
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error adding CN to X509_NAME");
    }

    if (domain_names.begin() != domain_names.end())
    {
        X509_EXTENSIONS * extensions(sk_X509_EXTENSION_new_null());

        std::string other_domain_names;
        for (auto it = domain_names.begin() + 1; it != domain_names.end(); ++it)
        {
            if (it->size() == 0)
                continue;
            other_domain_names += fmt::format("DNS:{}, ", *it);
        }

        auto * nid = X509V3_EXT_conf_nid(nullptr, nullptr, NID_subject_alt_name, other_domain_names.c_str());
        // ossl_check_X509_EXTENSION_type(nid);

        int ret = OPENSSL_sk_push(reinterpret_cast<OPENSSL_STACK *>(extensions), static_cast<const void *>(nid));

        // if (!sk_X509_EXTENSION_push(extensions, X509V3_EXT_conf_nid(
        //     nullptr,
        //     nullptr,
        //     NID_subject_alt_name,
        //     other_domain_names.c_str()
        // )))
        if (!ret)
        {
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Unable to add Subject Alternative Name to extensions");
        }

        if (X509_REQ_add_extensions(req, extensions) != 1)
        {
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Unable to add Subject Alternative Names to CSR");
        }
    }

    BIO * key_bio(BIO_new(BIO_s_mem()));
    if (PEM_write_bio_PrivateKey(key_bio, key, nullptr, nullptr, 0, nullptr, nullptr) != 1)
    {
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error writing private key to BIO");
    }
    std::string private_key;


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

    while ((bytes_read = BIO_read(key_bio, buffer, sizeof(buffer))) > 0)
    {
        private_key.append(buffer, bytes_read);
    }

    LOG_DEBUG(&Poco::Logger::get("ACME"), "CSR: {}", csr);
    LOG_DEBUG(&Poco::Logger::get("ACME"), "Private key: {}", private_key);

    return csr;
}

std::string generatePrivateKeyInPEM()
{
    auto key = Poco::Crypto::RSAKey(Poco::Crypto::RSAKey::KL_4096, Poco::Crypto::RSAKey::EXP_LARGE);

    BIO * key_bio(BIO_new(BIO_s_mem()));
    if (PEM_write_bio_RSAPrivateKey(key_bio, key.impl()->getRSA(), nullptr, nullptr, 0, nullptr, nullptr) != 1)
    {
        BIO_free(key_bio);
        // EVP_PKEY_free(pkey);
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Error writing private key to BIO: {}", getOpenSSLErrors());
    }

    unsigned char * data = nullptr;
    size_t data_len = 0;

    data_len = BIO_get_mem_data(key_bio, &data);
    std::string private_key(reinterpret_cast<char *>(data), data_len);

    BIO_free(key_bio);
    // EVP_PKEY_free(pkey);

    return private_key;
}

}

using DirectoryPtr = std::shared_ptr<Directory>;

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
    auto http_port = config.getInt("http_port");
    if (http_port != 80 && !initialized)
        LOG_WARNING(log, "For ACME HTTP challenge HTTP port must be 80, but is {}", http_port);

    directory_url = config.getString("acme.directory_url", LetsEncrypt::ACME_STAGING_DIRECTORY_URL);
    contact_email = config.getString("acme.email", "");
    terms_of_service_agreed = config.getBool("acme.terms_of_service_agreed");

    Poco::Util::AbstractConfiguration::Keys domains_keys;
    config.keys("acme.domains", domains_keys);

    std::vector<std::string> served_domains;
    for (const auto & key : domains_keys)
    {
        served_domains.push_back(config.getString("acme.domains." + key));
    }
    domains = served_domains;

    connection_timeout_settings = ConnectionTimeouts();
    proxy_configuration = ProxyConfiguration();

    auto context = Context::getGlobalContextInstance();
    auto zk = context->getZooKeeper();
    zk->createIfNotExists(fs::path(ZOOKEEPER_ACME_BASE_PATH), "");

    BackgroundSchedulePool & bgpool = context->getSchedulePool();


    auto do_election = [this, zk] {
        LOG_DEBUG(log, "Running election task");

        election_task->scheduleAfter(1000);

        /// fixme no throwing in BgSchPool
        auto leader = zkutil::EphemeralNodeHolder::tryCreate(fs::path(ZOOKEEPER_ACME_BASE_PATH) / "leader", *zk);
        if (leader)
            leader_node = std::move(leader);

        LOG_DEBUG(log, "I'm the leader: {}", leader_node ? "yes" : "no");

        /// TODO what if key changes?
        if (leader_node && !private_acme_key)
        {
            Coordination::Stat private_key_stat;
            std::string private_key;

            auto res = zk->tryGet(fs::path(ZOOKEEPER_ACME_BASE_PATH) / "account_private_key", private_key, &private_key_stat);
            if (!res)
            {
                LOG_DEBUG(log, "Generating new RSA private key for ACME account");
                private_key = generatePrivateKeyInPEM();

                /// TODO handle exception
                zk->createIfNotExists(fs::path(ZOOKEEPER_ACME_BASE_PATH) / "account_private_key", private_key);
            }

            chassert(!private_key.empty());

            std::istringstream private_key_stream(private_key);  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            private_acme_key = std::make_shared<Poco::Crypto::RSAKey>(nullptr, &private_key_stream, "");
        }

    };
    election_task = bgpool.createTask("ACMEClient", do_election);
    election_task->activate();

    do_election();

    refresh_task = bgpool.createTask(
        "ACMEClient",
        [this]
        {
            LOG_DEBUG(log, "Running ACMEClient task");

            refresh_task->scheduleAfter(1000);
        });

    refresh_task->activateAndSchedule();


    if (!initialized)
    {
        if (!directory)
            directory = getDirectory();

        if (!authenticated)
        {
            authenticate();
            authenticated = true;
        }

        chassert(!key_id.empty());

        // auto order_url = order();
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

    const auto nonce = requestNonce();

    auto uri = Poco::URI(url);

    auto jwk = JSONWebKey::fromRSAKey(*private_acme_key).toString();

    std::string protected_enc;
    {
        std::string protected_data;

        if (!key_id.empty())
            protected_data = fmt::format(R"({{"alg":"RS256","kid":"{}","nonce":"{}","url":"{}"}})", key_id, nonce, url);
        else
            protected_data = fmt::format(R"({{"alg":"RS256","jwk":{},"nonce":"{}","url":"{}"}})", jwk, nonce, url);

        protected_enc = base64Encode(protected_data, /*url_encoding*/ true, /*no_padding*/ true);
    }

    auto payload_enc = base64Encode(payload, /*url_encoding*/ true, /*no_padding*/ true);

    std::string to_sign = protected_enc + "." + payload_enc;
    auto signature = calculateHMACwithSHA256(to_sign, *private_acme_key);

    std::string request_data
        = R"({"protected":")" + protected_enc + R"(","payload":")" + payload_enc + R"(","signature":")" + signature + R"("})";


    auto r = Poco::Net::HTTPRequest(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery());
    r.set("Content-Type", "application/jose+json");
    r.set("Content-Length", std::to_string(request_data.size()));

    auto session = makeHTTPSession(
        HTTPConnectionGroupType::HTTP,
        uri,
        connection_timeout_settings,
        proxy_configuration
    );

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

    Poco::JSON::Object payload_object;
    auto contact = Poco::JSON::Array();

    if (!contact_email.empty())
        contact.add(contact_email);

    payload_object.set("contact", contact);
    payload_object.set("termsOfServiceAgreed", terms_of_service_agreed);

    std::ostringstream payload;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    payload.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(payload_object, payload);

    LOG_DEBUG(log, "Account payload: {}", payload.str());

    auto http_response = std::make_shared<Poco::Net::HTTPResponse>();
    auto response = doJWSRequest(directory->new_account, payload.str(), http_response);

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

    std::ostringstream payload;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
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
    std::string csr = generateCSR(domains);
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
    auto r = Poco::Net::HTTPRequest(Poco::Net::HTTPRequest::HTTP_HEAD, uri.getPathAndQuery());

    LOG_TRACE(log, "Requesting nonce from {}", uri.getPathAndQuery());

    auto session = makeHTTPSession(
        HTTPConnectionGroupType::HTTP,
        uri,
        connection_timeout_settings,
        proxy_configuration
    );
    session->sendRequest(r);

    auto response = Poco::Net::HTTPResponse();
    receiveResponse(*session, r, response, /* allow_redirects */ false);

    return response.get(NONCE_HEADER_NAME);
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

    LOG_DEBUG(log, "Challenge requested for uri: {}", uri);

    if (!uri.starts_with(ACME_CHALLENGE_HTTP_PATH))
        return "";

    auto host = Poco::URI(uri).getHost();
    LOG_DEBUG(log, "Host: {}", host);

    if (std::find(domains.begin(), domains.end(), host) == domains.end())
        return "";

    auto context = Context::getGlobalContextInstance();
    auto zk = context->getZooKeeper();

    Coordination::Stat challenge_stat;
    std::string challenge;

    auto response = zk->tryGet(fs::path(ZOOKEEPER_ACME_BASE_PATH) / host / "challenge", challenge, &challenge_stat);
    if (!response)
        return "";

    // std::string token_from_uri = uri.substr(std::string(ACME_CHALLENGE_HTTP_PATH).size());
    // auto jwk = JSONWebKey::fromRSAKey(*private_acme_key).toString();
    // auto jwk_thumbprint = encodeSHA256(jwk);
    // jwk_thumbprint = base64Encode(jwk_thumbprint, /*url_encoding*/ true, /*no_padding*/ true);
    //
    // return token_from_uri + "." + jwk_thumbprint;

    return challenge;
}

}
}
#endif
