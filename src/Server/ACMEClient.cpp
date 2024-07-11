#include <Server/ACMEClient.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/Credentials.h>
#include <Poco/String.h>
#include <fmt/core.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

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

void ACMEClient::reload(const Poco::Util::AbstractConfiguration & )
try
{
    if (!client)
    {
        client = std::make_unique<acme_lw::AcmeClient>(/*pem_signing_key*/"aaaaaaa");
        client->init(acme_lw::AcmeClient::Environment::STAGING); /// FIXME
    }

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
