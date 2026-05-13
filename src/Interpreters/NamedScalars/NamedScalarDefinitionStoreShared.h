#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/NamedScalars/INamedScalarDefinitionStore.h>

#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeperCachingGetter.h>

#include <memory>

namespace zkutil
{
class ZooKeeper;
using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

/// Keeper I/O for shared named-scalar definitions.
///
/// Layout under `<root>`:
///   `defs/<name>` -- text envelope with UUID, definer, and CREATE query.
class NamedScalarDefinitionStoreShared : public IWatchableNamedScalarDefinitionStore
{
public:
    NamedScalarDefinitionStoreShared(const ContextPtr & global_context_, const String & zookeeper_root_);

    void createRootNodesIfNeeded();

    bool isKeeperBacked() const override { return true; }
    void initialize() override { createRootNodesIfNeeded(); }
    std::vector<LoadedNamedScalarDefinition> loadAll() override;
    bool removeDefinition(const String & name, bool throw_if_not_exists) override;
    bool definitionExists(const String & name) override;
    size_t definitionCount();

    bool publishDefinition(
        const String & name,
        const String & definition_blob,
        bool if_not_exists,
        bool or_replace,
        const Settings & settings) override;

    Strings listDefinitionsWithChildrenWatch(std::function<void()> on_change) override;
    bool readDefinition(const String & name, String & out) override;
    bool readDefinitionWithDataWatch(
        const String & name,
        String & out,
        std::function<void()> on_change) override;

    zkutil::ZooKeeperPtr getZooKeeper();

private:
    String definitionsRootPath() const;
    String definitionPath(const String & name) const;

    String childGroup(const String & child) const
    {
        return zookeeper_root == "/" ? "/" + child : zookeeper_root + "/" + child;
    }

    ContextPtr global_context;
    String zookeeper_root;
    zkutil::ZooKeeperCachingGetter zookeeper_getter;
    LoggerPtr log;
};

using NamedScalarDefinitionStoreSharedPtr = std::shared_ptr<NamedScalarDefinitionStoreShared>;

}
