#pragma once

#include <vector>
#include <unordered_set>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
class StorageOfAllowedURL
{
public:
    void checkURL(const Poco::URI &uri); /// If URL not allowed in config.xml throw UNACCEPTABLE_URL Exception

    void setValuesFromConfig(const Poco::Util::AbstractConfiguration &config);

    void checkHostAndPort(const std::string & host, const std::string & port);

private:
    std::unordered_set<std::string> primary_hosts;      /// Allowed primary (<host>) URL from config.xml
    std::vector<std::string> regexp_hosts;              /// Allowed regexp (<hots_regexp>) URL from config.xml

    bool checkString(const std::string &host);
};
}
