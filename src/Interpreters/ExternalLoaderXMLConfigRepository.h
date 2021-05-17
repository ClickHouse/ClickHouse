#pragma once

#include <common/types.h>
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
    ExternalLoaderXMLConfigRepository(const Poco::Util::AbstractConfiguration & main_config_, const std::string & config_key_);

    std::string getName() const override { return name; }

    /// Return set of .xml files from path in main_config (config_key)
    std::set<std::string> getAllLoadablesDefinitionNames() override;

    /// Checks that file with name exists on filesystem
    bool exists(const std::string & definition_entity_name) override;

    /// Return xml-file modification time via stat call
    Poco::Timestamp getUpdateTime(const std::string & definition_entity_name) override;

    /// May contain definition about several entities (several dictionaries in one .xml file)
    LoadablesConfigurationPtr load(const std::string & config_file) override;

private:
    const String name;

    /// Main server config (config.xml).
    const Poco::Util::AbstractConfiguration & main_config;

    /// Key which contains path to directory with .xml configs for entries
    std::string config_key;
};

}
