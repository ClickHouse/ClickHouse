#pragma once

#include <string>
#include <boost/core/noncopyable.hpp>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Logger.h>

#include <acme-lw.h>

namespace DB
{

namespace ACMEClient
{

static constexpr auto ACME_CHALLENGE_PATH = "/.well-known/acme-challenge";

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

    std::vector<std::string> domains;
};

}
}
