#pragma once

#include <string>
#include <boost/core/noncopyable.hpp>
#include <Poco/Crypto/RSAKey.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "Common/ZooKeeper/ZooKeeper.h"
#include <Common/Logger.h>
#include "Core/BackgroundSchedulePool.h"

#include <acme-lw.h>

namespace DB
{

namespace ACMEClient
{

static constexpr auto ACME_CHALLENGE_PATH = "/.well-known/acme-challenge";
static constexpr auto ZOOKEEPER_ACME_BASE_PATH = "/clickhouse/acme";

/// A singleton
class ACMEClient : private boost::noncopyable
{
public:
    static ACMEClient & instance();

    void reload(const Poco::Util::AbstractConfiguration & config);
    std::string requestChallenge(const std::string & uri);

    void dummyCallback(const std::string & domain_name, const std::string & url, const std::string & key);
private:
    ACMEClient() = default;

    LoggerPtr log = getLogger("ACMEClient");

    bool initialized;
    std::unique_ptr<acme_lw::AcmeClient> client;

    BackgroundSchedulePoolTaskHolder election_task;
    BackgroundSchedulePoolTaskHolder refresh_task;

    zkutil::EphemeralNodeHolderPtr leader_node;

    std::vector<std::string> domains;
    std::string requestNonce();
    void getDirectory();
    void authenticate(Poco::Crypto::RSAKey & cert);
};

}
}
