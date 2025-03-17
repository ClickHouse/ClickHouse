#pragma once

#include <base/types.h>
#include <unordered_set>
#include <mutex>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Poco/Timestamp.h>


namespace DB
{

/// XML config repository used by ExternalLoader.
/// Represents xml-files in local filesystem.
class ExternalLoaderXMLConfigRepository : public IExternalLoaderConfigRepository
{
public:
    ExternalLoaderXMLConfigRepository(const std::string & app_path_, const std::string & main_config_path_, const std::unordered_set<std::string> & patterns_);

    std::string getName() const override { return name; }

    /// Return set of .xml files from path in main_config (config_key)
    std::set<std::string> getAllLoadablesDefinitionNames() override;

    /// Checks that file with name exists on filesystem
    bool exists(const std::string & definition_entity_name) override;

    /// Return xml-file modification time via stat call
    std::optional<Poco::Timestamp> getUpdateTime(const std::string & definition_entity_name) override;

    /// May contain definition about several entities (several dictionaries in one .xml file)
    LoadablesConfigurationPtr load(const std::string & config_file) override;

    void updatePatterns(const std::unordered_set<std::string> & patterns_);

private:

    const String name;

    const std::string app_path;

    const std::string main_config_path;

    std::unordered_set<std::string> patterns;

    mutable std::mutex patterns_mutex;

};

}
