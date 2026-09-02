#pragma once
#include <string>
#include <vector>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}
namespace DB
{
/// get all internal key names for given key
std::vector<std::string> getMultipleKeysFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & root, const std::string & name);
/// Get all values for given key
std::vector<std::string> getMultipleValuesFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & root, const std::string & name);
}
