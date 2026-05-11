#pragma once

#include <Core/Types.h>
#include <base/types.h>

#include <functional>
#include <memory>
#include <vector>

namespace DB
{

struct Settings;

struct LoadedNamedScalarDefinition
{
    String name;
    String definition_blob;
};

class INamedScalarDefinitionStore
{
public:
    virtual ~INamedScalarDefinitionStore() = default;

    virtual bool isKeeperBacked() const = 0;
    virtual void initialize() = 0;
    virtual std::vector<LoadedNamedScalarDefinition> loadAll() = 0;

    virtual bool definitionExists(const String & name) = 0;
    virtual bool publishDefinition(
        const String & name,
        const String & definition_blob,
        bool if_not_exists,
        bool or_replace,
        const Settings & settings) = 0;

    virtual bool removeDefinition(const String & name, bool throw_if_not_exists) = 0;
    virtual bool readDefinition(const String & name, String & out) = 0;
};

class IWatchableNamedScalarDefinitionStore : public INamedScalarDefinitionStore
{
public:
    virtual Strings listDefinitionsWithChildrenWatch(std::function<void()> on_change) = 0;
    virtual bool readDefinitionWithDataWatch(
        const String & name,
        String & out,
        std::function<void()> on_change) = 0;
};

using NamedScalarDefinitionStorePtr = std::shared_ptr<INamedScalarDefinitionStore>;
using WatchableNamedScalarDefinitionStorePtr = std::shared_ptr<IWatchableNamedScalarDefinitionStore>;

}
