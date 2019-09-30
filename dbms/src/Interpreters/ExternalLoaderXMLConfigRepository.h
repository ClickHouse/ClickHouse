#pragma once

#include <Core/Types.h>
#include <unordered_map>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Poco/Timestamp.h>

namespace DB
{

/// XML config repository used by ExternalLoader.
/// Represents xml-files in local filesystem.
class ExternalLoaderXMLConfigRepository : public IExternalLoaderConfigRepository
{
public:

    ExternalLoaderXMLConfigRepository(const Poco::Util::AbstractConfiguration & main_config_, const std::string & config_key_)
        : main_config(main_config_)
        , config_key(config_key_)
    {
    }

    /// Return set of .xml files from path in main_config (config_key)
    std::set<std::string> getAllLoadablesDefinitionNames() const override;

    /// Checks that file with name exists on filesystem
    bool exists(const std::string & definition_entity_name) const override;

    /// Checks that file was updated since last check
    bool isUpdated(const std::string & definition_entity_name) override;

    /// May contain definition about several entities (several dictionaries in one .xml file)
    LoadablesConfigurationPtr load(const std::string & definition_entity_name) const override;

private:

    /// Simple map with last modification time with path -> timestamp,
    /// modification time received by stat.
    std::unordered_map<std::string, Poco::Timestamp> update_time_mapping;

    /// Main server config (config.xml).
    const Poco::Util::AbstractConfiguration & main_config;

    /// Key which contains path to dicrectory with .xml configs for entries
    std::string config_key;
};

}
