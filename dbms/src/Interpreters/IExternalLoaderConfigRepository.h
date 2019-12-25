#pragma once

#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Timestamp.h>

#include <memory>
#include <string>
#include <set>

namespace DB
{

using LoadablesConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

/// Base interface for configurations source for Loadble objects, which can be
/// loaded with ExternalLoader. Configurations may came from filesystem (XML-files),
/// server memory (from database), etc. It's important that main result of this class
/// (LoadablesConfigurationPtr) may contain more than one loadable config,
/// each one with own key, which can be obtained with keys method,
/// for example multiple dictionaries can be defined in single .xml file.
class IExternalLoaderConfigRepository
{
public:
    /// Return all available sources of loadables structure
    /// (all .xml files from fs, all entities from database, etc)
    virtual std::set<std::string> getAllLoadablesDefinitionNames() const = 0;

    /// Checks that source of loadables configuration exist.
    virtual bool exists(const std::string & loadable_definition_name) const = 0;

    /// Returns entity last update time
    virtual Poco::Timestamp getUpdateTime(const std::string & loadable_definition_name) = 0;

    /// Load configuration from some concrete source to AbstractConfiguration
    virtual LoadablesConfigurationPtr load(const std::string & loadable_definition_name) const = 0;

    virtual ~IExternalLoaderConfigRepository() = default;

    static const char * INTERNAL_REPOSITORY_NAME_PREFIX;
};

using ExternalLoaderConfigRepositoryPtr = std::unique_ptr<IExternalLoaderConfigRepository>;

}
