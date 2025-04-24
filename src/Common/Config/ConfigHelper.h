#pragma once

#include <Poco/AutoPtr.h>
#include <string>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB::ConfigHelper
{

/// Clones a configuration.
/// NOTE: This function assumes the source configuration doesn't have items having both children and a value
/// (i.e. items like "<test>value<child1/></test>").
Poco::AutoPtr<Poco::Util::AbstractConfiguration> clone(const Poco::Util::AbstractConfiguration & src);

/// The behavior is like `config.getBool(key, default_)`,
/// except when the tag is empty (aka. self-closing), `empty_as` will be used instead of throwing Poco::Exception.
bool getBool(const Poco::Util::AbstractConfiguration & config, const std::string & key, bool default_ = false, bool empty_as = true);

}
