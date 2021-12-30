#pragma once

namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB::ConfigHelper
{

bool getBool(const Poco::Util::AbstractConfiguration & config, const std::string & key, bool default_, bool empty_as);

}
