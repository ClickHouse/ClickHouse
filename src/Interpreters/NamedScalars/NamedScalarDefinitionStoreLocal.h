#pragma once

#include <Interpreters/NamedScalars/INamedScalarDefinitionStore.h>

#include <Common/Logger.h>

namespace DB
{

class NamedScalarDefinitionStoreLocal : public INamedScalarDefinitionStore
{
public:
    NamedScalarDefinitionStoreLocal(String dir_path_, LoggerPtr log_);

    const String & directoryPath() const { return dir_path; }

    bool isKeeperBacked() const override { return false; }
    void initialize() override;
    std::vector<LoadedNamedScalarDefinition> loadAll() override;
    bool definitionExists(const String & name) override;

    bool publishDefinition(
        const String & name,
        const String & definition_blob,
        bool if_not_exists,
        bool or_replace,
        const Settings & settings) override;

    bool removeDefinition(const String & name, bool throw_if_not_exists) override;
    bool readDefinition(const String & name, String & out) override;

private:
    String dir_path;
    LoggerPtr log;
};

}
