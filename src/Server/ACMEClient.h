#pragma once

#include <boost/core/noncopyable.hpp>

#include <Core/BackgroundSchedulePool.h>
#include <Poco/Crypto/RSAKey.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/JSONWebKey.h>
#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

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
};


static constexpr auto ACME_CHALLENGE_HTTP_PATH = "/.well-known/acme-challenge/";
static constexpr auto ZOOKEEPER_ACME_BASE_PATH = "/clickhouse/acme";

static constexpr auto HTTP_01_CHALLENGE_TYPE = "http-01";

class ACMEClient : private boost::noncopyable
{
public:
    static ACMEClient & instance();

    void reload(const Poco::Util::AbstractConfiguration & config);
    std::string requestChallenge(const std::string & uri);

    void requestCertificate(const Poco::Util::AbstractConfiguration & config);
private:
    ACMEClient() = default;

    LoggerPtr log = getLogger("ACMEClient");

    std::atomic<bool> initialized;
    std::atomic<bool> authenticated;

    /// Private key identifier, local to ACME provider
    std::string key_id;
    std::shared_ptr<Poco::Crypto::RSAKey> private_acme_key;

    std::shared_ptr<Directory> directory;

    BackgroundSchedulePoolTaskHolder election_task;
    BackgroundSchedulePoolTaskHolder refresh_task;

    zkutil::EphemeralNodeHolderPtr leader_node;

    std::vector<std::string> domains;

    std::string requestNonce();
    void getDirectory();
    void authenticate();
    std::string order();
    void finalizeOrder(const std::string &);
    void processAuthorization(const std::string & auth_url);
    void tryGet(const std::string & finalize_url, Poco::Crypto::RSAKey & key);
    std::string doJWSRequest(const std::string &, const std::string &, std::shared_ptr<Poco::Net::HTTPResponse>);
};

}
}
