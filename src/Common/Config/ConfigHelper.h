#pragma once

#include <string>

namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB::ConfigHelper
{

/// The behavior is like `config.getBool(key, default_)`,
/// except when the tag is empty (aka. self-closing), `empty_as` will be used instead of throwing Poco::Exception.
bool getBool(const Poco::Util::AbstractConfiguration & config, const std::string & key, bool default_ = false, bool empty_as = true);

}
