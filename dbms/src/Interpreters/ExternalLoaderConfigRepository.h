#pragma once

#include <Interpreters/IExternalLoaderConfigRepository.h>

namespace DB
{

/** Default implementation of config repository used by native server application.
  * Represents files in local filesystem.
  */
class ExternalLoaderConfigRepository : public IExternalLoaderConfigRepository
{
public:
    Files list(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & path_key) const override;

    bool exists(const std::string & config_file) const override;

    Poco::Timestamp getLastModificationTime(const std::string & config_file) const override;

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> load(const std::string & config_file) const override;
};

}
