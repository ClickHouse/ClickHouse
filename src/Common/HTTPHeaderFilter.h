#pragma once

#include <IO/HTTPHeaderEntries.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <vector>
#include <unordered_set>
#include <mutex>


namespace DB
{

class HTTPHeaderFilter
{
public:

    void setValuesFromConfig(const Poco::Util::AbstractConfiguration & config);
    void checkAndNormalizeHeaders(HTTPHeaderEntries & entries) const;

private:
    std::unordered_set<std::string> forbidden_headers;
    std::vector<std::string> forbidden_headers_regexp;

    mutable std::mutex mutex;
};

}
