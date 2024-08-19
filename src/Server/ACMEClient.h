#pragma once

#include <boost/core/noncopyable.hpp>

#include <Core/BackgroundSchedulePool.h>
#include <Poco/Crypto/RSAKey.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ProxyConfiguration.h>
#include <Common/JSONWebKey.h>
#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <IO/ConnectionTimeouts.h>


namespace DB
{

namespace LetsEncrypt
{
    static constexpr auto ACME_STAGING_DIRECTORY_URL = "https://acme-staging-v02.api.letsencrypt.org/directory";
    static constexpr auto ACME_PRODUCTION_DIRECTORY_URL = "https://acme-v02.api.letsencrypt.org/directory";
}

namespace ACMEClient
{

struct Directory
{
    static constexpr auto new_account_key = "newAccount";
    static constexpr auto new_order_key = "newOrder";
    static constexpr auto new_nonce_key = "newNonce";

    std::string new_account;
    std::string new_order;
    std::string new_nonce;

    static Directory parse(const std::string & json_data)
    {
        Poco::JSON::Parser parser;
        auto json = parser.parse(json_data).extract<Poco::JSON::Object::Ptr>();

        auto dir = Directory{
            .new_account = json->getValue<std::string>(Directory::new_account_key),
            .new_order = json->getValue<std::string>(Directory::new_order_key),
            .new_nonce = json->getValue<std::string>(Directory::new_nonce_key),
        };

        LOG_DEBUG(
            &Poco::Logger::get("ACMEClient::Directory"),
            "Directory: newAccount: {}, newOrder: {}, newNonce: {}",
            dir.new_account,
            dir.new_order,
            dir.new_nonce
        );

        return dir;
    }
};


static constexpr auto ACME_CHALLENGE_HTTP_PATH = "/.well-known/acme-challenge/";
static constexpr auto ZOOKEEPER_ACME_BASE_PATH = "/clickhouse/acme";

static constexpr auto HTTP_01_CHALLENGE_TYPE = "http-01";

static constexpr auto NONCE_HEADER_NAME = "replay-nonce";

class ACMEClient : private boost::noncopyable
{
    using DirectoryPtr = std::shared_ptr<Directory>;

public:
    static ACMEClient & instance();

    void reload(const Poco::Util::AbstractConfiguration & config);
    std::string requestChallenge(const std::string & uri);

    void requestCertificate(const Poco::Util::AbstractConfiguration & config);
private:
    ACMEClient() = default;

    LoggerPtr log = getLogger("ACMEClient");

    std::atomic<bool> initialized;
    std::atomic<bool> keys_initialized;
    std::atomic<bool> authenticated;

    bool terms_of_service_agreed;
    std::string directory_url;
    std::string contact_email;

    ConnectionTimeouts connection_timeout_settings;
    ProxyConfiguration proxy_configuration;

    /// Private key identifier, local to ACME provider
    /// Also may be used as an account URL
    std::string key_id;
    std::shared_ptr<Poco::Crypto::RSAKey> private_acme_key;

    DirectoryPtr directory;

    BackgroundSchedulePoolTaskHolder election_task;
    BackgroundSchedulePoolTaskHolder refresh_task;

    zkutil::EphemeralNodeHolderPtr leader_node;

    std::vector<std::string> domains;

    std::string requestNonce();
    DirectoryPtr getDirectory();
    void authenticate();
    std::string order();
    void finalizeOrder(const std::string &);
    void processAuthorization(const std::string & auth_url);
    void tryGet(const std::string & finalize_url, Poco::Crypto::RSAKey & key);
    std::string doJWSRequest(const std::string &, const std::string &, std::shared_ptr<Poco::Net::HTTPResponse>);
};

}
}
