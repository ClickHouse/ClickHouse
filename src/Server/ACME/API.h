#pragma once

#include "config.h"

#if USE_SSL
#include <Common/Crypto/KeyPair.h>
#include <Common/Crypto/X509Certificate.h>
#include <Common/ProxyConfiguration.h>
#include <Common/logger_useful.h>
#include <IO/ConnectionTimeouts.h>

#include <Poco/JSON/Parser.h>

#include <boost/core/noncopyable.hpp>

#include <memory>
#include <string>

namespace DB
{

namespace ACME
{

namespace LetsEncrypt
{
    static constexpr auto STAGING_DIRECTORY_URL = "https://acme-staging-v02.api.letsencrypt.org/directory";
    static constexpr auto PRODUCTION_DIRECTORY_URL = "https://acme-v02.api.letsencrypt.org/directory";
}

static constexpr auto HTTP_01_CHALLENGE_TYPE = "http-01";
static constexpr auto NONCE_HEADER_NAME = "replay-nonce";
static constexpr auto MAILTO_PREFIX = "mailto";
static constexpr auto APPLICATION_JOSE_JSON = "application/jose+json";

struct Order
{
    std::string status;

    const Poco::URI order_url;
    const Poco::URI finalize_url;
    const Poco::URI certificate_url;
};

struct Directory
{
    static constexpr auto new_account_key = "newAccount";
    static constexpr auto new_order_key = "newOrder";
    static constexpr auto new_nonce_key = "newNonce";

    const Poco::URI new_account;
    const Poco::URI new_order;
    const Poco::URI new_nonce;

    static Directory parse(const std::string & json_data);
};

using DirectoryPtr = std::shared_ptr<Directory>;
using Domains = std::vector<std::string>;
using OrderCallback = std::function<void(std::string)>;

class API : private boost::noncopyable
{
public:
    struct Configuration
    {
        const Poco::URI directory_url;
        const std::string contact_email;
        const bool terms_of_service_agreed;

        const std::shared_ptr<KeyPair> private_key;
    };

    explicit API(Configuration);

    bool isReady() const { return !key_id.empty(); }

    DirectoryPtr getDirectory() const;
    std::string order(const Domains &, OrderCallback) const;
    Order describeOrder(const Poco::URI &) const;
    std::string pullCertificate(const Poco::URI &) const;
    bool finalizeOrder(const Poco::URI &, const Domains &, const KeyPair &) const;
    void processAuthorization(const Poco::URI &, OrderCallback) const;

private:
    Configuration configuration;

    DirectoryPtr directory;
    std::string key_id;

    ConnectionTimeouts connection_timeout_settings;
    ProxyConfiguration proxy_configuration;

    LoggerPtr log = getLogger("ACME::API");

    std::string authenticate();
    std::string requestNonce() const;
    std::string doJWSRequest(const Poco::URI &, const std::string &, std::shared_ptr<Poco::Net::HTTPResponse>) const;
    std::string doJWSRequestWithEmptyPayload(const Poco::URI &) const;
    std::string formatJWSRequestData(const Poco::URI &, const std::string &, const std::string &) const;
    Poco::JSON::Object::Ptr doJWSRequestExpectingJSON(const Poco::URI &, const std::string &, std::shared_ptr<Poco::Net::HTTPResponse>) const;
};

}
}
#endif

