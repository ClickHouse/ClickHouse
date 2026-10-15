#pragma once

#include <base/types.h>
#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Poco/Timestamp.h>


namespace DB
{
/// A config repository filled with preset loadables used by ExternalLoader.
class ExternalLoaderTempConfigRepository : public IExternalLoaderConfigRepository
{
public:
    ExternalLoaderTempConfigRepository(const String & repository_name_, const String & path_, const LoadablesConfigurationPtr & config_);

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] bool isTemporary() const override { return true; }

    std::set<String> getAllLoadablesDefinitionNames() override;
    bool exists(const String & path) override;
    LoadablesConfigurationPtr load(const String & path) override;

private:
    String name;
    String path;
    LoadablesConfigurationPtr config;
};

}
