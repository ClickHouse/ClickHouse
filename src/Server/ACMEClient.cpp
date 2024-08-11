#include <Server/ACMEClient.h>

#include "Common/ZooKeeper/ZooKeeper.h"
#include <Common/logger_useful.h>
#include "Core/BackgroundSchedulePool.h"
#include "Interpreters/Context.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/Credentials.h>
#include <Poco/Crypto/X509Certificate.h>
#include <Poco/File.h>
#include <Poco/String.h>
#include <fmt/core.h>


namespace DB
{

// namespace ErrorCodes
// {
//     // extern const int LOGICAL_ERROR;
// }

namespace ACMEClient
{

namespace
{

void dumberCallback(const std::string & domain_name, const std::string & url, const std::string & key)
{
    ACMEClient::instance().dummyCallback(domain_name, url, key);
}

}


ACMEClient & ACMEClient::instance()
{
    static ACMEClient instance;
    return instance;
}

void ACMEClient::reload(const Poco::Util::AbstractConfiguration &)
try
{
    if (!client)
    {
        acme_lw::AcmeClient::init(acme_lw::AcmeClient::Environment::STAGING);

        auto cert_buf = ReadBufferFromFile("/home/thevar1able/src/clickhouse/cmake-build-debug/acme/acme.key");
        std::string cert_pem;
        readStringUntilEOF(cert_pem, cert_buf);
        LOG_DEBUG(log, "Read certificate from file: {}", cert_pem);

        client = std::make_unique<acme_lw::AcmeClient>(cert_pem);
    }

    auto context = Context::getGlobalContextInstance();
    auto zk = context->getZooKeeper();

    zk->createIfNotExists("/acme", "");

    BackgroundSchedulePool & bgpool = context->getSchedulePool();

    election_task = bgpool.createTask("ACMEClient", [this, zk] {
        LOG_DEBUG(log, "Running election task");

        election_task->scheduleAfter(1000);

        auto leader = zkutil::EphemeralNodeHolder::tryCreate("/acme/leader", *zk);
        if (leader)
            leader_node = std::move(leader);

        LOG_DEBUG(log, "I'm the leader: {}", leader_node ? "yes" : "no");
    });
    election_task->activateAndSchedule();

    refresh_task = bgpool.createTask("ACMEClient", [this] {
        LOG_DEBUG(log, "Running ACMEClient task");

        refresh_task->scheduleAfter(1000);
    });

    refresh_task->activateAndSchedule();

    initialized = true;
}
catch (...)
{
    tryLogCurrentException("Failed :(");
}

void ACMEClient::dummyCallback(const std::string & domain_name, const std::string & url, const std::string & key)
{
    LOG_DEBUG(log, "Callback for domain {} with url {} and key {}", domain_name, url, key);
}

std::string ACMEClient::requestChallenge(const std::string & uri)
{
    LOG_DEBUG(log, "Requesting challenge for {}", uri);

    client->issueCertificate({domains.begin(), domains.end()}, dumberCallback);

    return "";
}

}
}
