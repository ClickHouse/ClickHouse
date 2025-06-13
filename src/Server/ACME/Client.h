#pragma once

#include "config.h"

#if USE_SSL
#include <boost/core/noncopyable.hpp>

#include <Common/Crypto/KeyPair.h>
#include <Common/Crypto/X509Certificate.h>
#include <Common/JSONWebKey.h>
#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperLock.h>
#include <Core/BackgroundSchedulePool.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Server/ACME/API.h>


namespace DB
{

namespace ACME
{

static constexpr auto CHALLENGE_HTTP_PATH = "/.well-known/acme-challenge/";
static constexpr auto ZOOKEEPER_BASE_PATH = "/clickhouse/acme";

static constexpr auto REFRESH_TASK_AFTER_ERROR_MS = 10000;
static constexpr auto REFRESH_TASK_HAPPY_PATH_MS = 5000;
static constexpr auto REFRESH_TASK_IN_A_SECOND = 1000;


struct VersionedCertificate
{
    KeyPair private_key;
    X509Certificate::List certificate;

    std::string version;
};


class Client : private boost::noncopyable
{
public:
    static Client & instance();
    void shutdown();

    void initialize(const Poco::Util::AbstractConfiguration & config);
    std::string requestChallenge(const std::string & uri);

    std::optional<VersionedCertificate> requestCertificate() const;

private:
    Client() = default;

    LoggerPtr log = getLogger("ACME::Client");

    std::atomic<bool> initialized;
    std::atomic<bool> keys_initialized;
    std::atomic<bool> authenticated;

    std::shared_ptr<ACME::API> api;

    bool terms_of_service_agreed;
    Poco::URI directory_url;
    std::string acme_hostname;
    std::string contact_email;

    std::shared_ptr<KeyPair> private_acme_key TSA_GUARDED_BY(private_acme_key_mutex);
    std::mutex private_acme_key_mutex;

    BackgroundSchedulePoolTaskHolder authentication_task;
    BackgroundSchedulePoolTaskHolder refresh_key_task;
    BackgroundSchedulePoolTaskHolder refresh_certificates_task;

    void refresh_key_fn();
    void authentication_fn();
    void refresh_certificates_fn(const Poco::Util::AbstractConfiguration & config);

    std::shared_ptr<zkutil::ZooKeeperLock> lock;

    std::vector<std::string> domains;
    UInt64 refresh_certificates_task_interval;
    UInt64 refresh_certificates_before;
    std::optional<std::string> active_order;
};

}
}
#endif
