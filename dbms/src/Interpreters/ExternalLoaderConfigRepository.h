#pragma once

#include <Poco/AutoPtr.h>
#include <Poco/Timestamp.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <set>
#include <string>

namespace DB
{

/** Config repository used by native server application.
  * Represents files in local filesystem.
  */
class ExternalLoaderConfigRepository
{
public:
    using Files = std::set<std::string>;
    Files list(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & path_key) const;

    bool exists(const std::string & config_file) const;

    Poco::Timestamp getLastModificationTime(const std::string & config_file) const;

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> load(const std::string & config_file, const std::string & preprocessed_dir = "") const;
};

}
