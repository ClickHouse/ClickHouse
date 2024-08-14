#pragma once

#include <boost/core/noncopyable.hpp>

#include <Core/BackgroundSchedulePool.h>
#include <Poco/Crypto/RSAKey.h>
#include <Poco/Crypto/X509Certificate.h>
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


static constexpr auto ACME_CHALLENGE_HTTP_PATH = "/.well-known/acme-challenge";
static constexpr auto ZOOKEEPER_ACME_BASE_PATH = "/clickhouse/acme";


class ACMEClient : private boost::noncopyable
{
public:
    static ACMEClient & instance();

    void reload(const Poco::Util::AbstractConfiguration & config);
    std::string requestChallenge(const std::string & uri);

private:
    ACMEClient() = default;

    LoggerPtr log = getLogger("ACMEClient");

    std::atomic<bool> initialized;

    /// Private key identifier, local to ACME provider
    std::string key_id;

    std::shared_ptr<Directory> directory;

    BackgroundSchedulePoolTaskHolder election_task;
    BackgroundSchedulePoolTaskHolder refresh_task;

    zkutil::EphemeralNodeHolderPtr leader_node;

    std::vector<std::string> domains;

    std::string requestNonce();
    void getDirectory();
    void authenticate(Poco::Crypto::RSAKey &);
};

}
}
