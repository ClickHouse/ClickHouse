#pragma once

#include <Core/Types.h>
#include <unordered_map>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Poco/Timestamp.h>


namespace DB
{
/// A config repository filled with preset loadables used by ExternalLoader.
class ExternalLoaderPresetConfigRepository : public IExternalLoaderConfigRepository
{
public:
    ExternalLoaderPresetConfigRepository(const std::vector<std::pair<String, LoadablesConfigurationPtr>> & preset_);
    ~ExternalLoaderPresetConfigRepository() override;

    std::set<String> getAllLoadablesDefinitionNames() const override;
    bool exists(const String & path) const override;
    Poco::Timestamp getUpdateTime(const String & path) override;
    LoadablesConfigurationPtr load(const String & path) const override;

private:
    std::unordered_map<String, LoadablesConfigurationPtr> preset;
    Poco::Timestamp creation_time;
};

}
