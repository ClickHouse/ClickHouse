#pragma once

#include <unordered_set>
#include <Poco/Crypto/EVPPKey.h>
#include "config.h"

#include <Common/ZooKeeper/ZooKeeperLock.h>

#if USE_SSL
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

struct ACMEOrder
{
    std::string status;

    std::string order_url;
    std::string finalize_url;
    std::string certificate_url;
};


static constexpr auto ACME_CHALLENGE_HTTP_PATH = "/.well-known/acme-challenge/";

///  TODO should we include provider name here?
///  /clickhouse/acme
///     /account_private_key
///     /challenges
///         /test.example.com
///
static constexpr auto ZOOKEEPER_ACME_BASE_PATH = "/clickhouse/acme";

static constexpr auto HTTP_01_CHALLENGE_TYPE = "http-01";

static constexpr auto NONCE_HEADER_NAME = "replay-nonce";

class ACMEClient : private boost::noncopyable
{
    using DirectoryPtr = std::shared_ptr<Directory>;

public:
    static ACMEClient & instance();

    void initialize(const Poco::Util::AbstractConfiguration & config);
    void reload(const Poco::Util::AbstractConfiguration & config);
    std::string requestChallenge(const std::string & uri);

    bool isReady() const { return false; }

    std::optional<std::tuple<Poco::Crypto::EVPPKey, Poco::Crypto::X509Certificate>> requestCertificate(const Poco::Util::AbstractConfiguration & config);
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
    std::mutex private_acme_key_mutex;

    std::string acme_hostname;
    DirectoryPtr directory;

    BackgroundSchedulePoolTaskHolder authentication_task;
    BackgroundSchedulePoolTaskHolder refresh_key_task;
    BackgroundSchedulePoolTaskHolder refresh_certificates_task;

    zkutil::ZooKeeperPtr zookeeper;
    std::shared_ptr<zkutil::ZooKeeperLock> lock;
    zkutil::EphemeralNodeHolderPtr lock2;

    std::vector<std::string> domains;
    UInt64 refresh_certificates_interval;
    std::optional<std::string> active_order;

    std::string requestNonce();
    DirectoryPtr getDirectory();
    void authenticate();
    std::string order();
    void finalizeOrder(const std::string &, const std::string &);
    void processAuthorization(const std::string & auth_url);
    void tryGet(const std::string & finalize_url, Poco::Crypto::RSAKey & key);
    std::string doJWSRequest(const std::string &, const std::string &, std::shared_ptr<Poco::Net::HTTPResponse>);
    ACMEOrder describeOrder(const std::string & order_url);
    std::string pullCertificate(const std::string & certificate_url);
};

}
}
#endif
