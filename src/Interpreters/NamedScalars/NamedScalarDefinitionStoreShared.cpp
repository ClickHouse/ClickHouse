#include <Interpreters/NamedScalars/NamedScalarDefinitionStoreShared.h>

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NAMED_SCALAR_ALREADY_EXISTS;
    extern const int NAMED_SCALAR_NOT_FOUND;
    extern const int NO_ZOOKEEPER;
    extern const int KEEPER_EXCEPTION;
}

namespace FailPoints
{
    extern const char shared_named_scalars_store_value_fail_once[];
}

namespace
{

zkutil::GetZooKeeper makeGetZooKeeper(const ContextPtr & context)
{
    std::weak_ptr<const Context> weak_ctx = context;
    return [weak_ctx]() -> zkutil::ZooKeeperPtr
    {
        auto locked = weak_ctx.lock();
        if (!locked)
            throw Exception(ErrorCodes::NO_ZOOKEEPER, "Global context for shared named_scalars definition storage is gone");
        return locked->getZooKeeper();
    };
}

}

NamedScalarDefinitionStoreShared::NamedScalarDefinitionStoreShared(
    const ContextPtr & global_context_,
    const String & zookeeper_root_)
    : global_context(global_context_)
    , zookeeper_root(zookeeper_root_)
    , zookeeper_getter(makeGetZooKeeper(global_context_))
    , log(getLogger("NamedScalarDefinitionStoreShared"))
{
    while (!zookeeper_root.empty() && zookeeper_root.back() == '/')
        zookeeper_root.pop_back();
    if (zookeeper_root.empty())
        zookeeper_root = "/";
}

zkutil::ZooKeeperPtr NamedScalarDefinitionStoreShared::getZooKeeper()
{
    auto [zookeeper, session_status] = zookeeper_getter.getZooKeeper();
    if (session_status == zkutil::ZooKeeperCachingGetter::SessionStatus::New)
    {
        /// We may have reconnected to a different Keeper member; sync to
        /// guarantee read-your-writes after failover.
        zookeeper->sync(zookeeper_root);
        createRootNodesIfNeeded();
    }
    return zookeeper;
}

String NamedScalarDefinitionStoreShared::definitionsRootPath() const
{
    return childGroup("defs");
}

String NamedScalarDefinitionStoreShared::definitionPath(const String & name) const
{
    return definitionsRootPath() + "/" + escapeForFileName(name);
}

void NamedScalarDefinitionStoreShared::createRootNodesIfNeeded()
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarDefinitionStoreShared::createRootNodesIfNeeded");
    auto [zookeeper, _] = zookeeper_getter.getZooKeeper();
    zookeeper->createAncestors(zookeeper_root);
    zookeeper->createIfNotExists(zookeeper_root, "");
    zookeeper->createIfNotExists(definitionsRootPath(), "");
}

bool NamedScalarDefinitionStoreShared::definitionExists(const String & name)
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarDefinitionStoreShared::definitionExists");
    auto zookeeper = getZooKeeper();
    return zookeeper->exists(definitionPath(name));
}

size_t NamedScalarDefinitionStoreShared::definitionCount()
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarDefinitionStoreShared::definitionCount");
    auto zookeeper = getZooKeeper();
    return zookeeper->getChildren(definitionsRootPath()).size();
}

bool NamedScalarDefinitionStoreShared::removeDefinition(const String & name, bool throw_if_not_exists)
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarDefinitionStoreShared::removeDefinition");
    auto zookeeper = getZooKeeper();
    const auto def_path = definitionPath(name);
    Coordination::Error code = zookeeper->tryRemoveRecursive(def_path);
    if (code == Coordination::Error::ZOK)
        return true;
    if (code == Coordination::Error::ZNONODE)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::NAMED_SCALAR_NOT_FOUND, "Shared scalar '{}' doesn't exist", name);
        return false;
    }
    throw zkutil::KeeperException::fromPath(code, def_path);
}

bool NamedScalarDefinitionStoreShared::publishDefinition(
    const String & name,
    const String & definition_blob,
    bool if_not_exists,
    bool or_replace,
    const Settings &)
{
    fiu_do_on(FailPoints::shared_named_scalars_store_value_fail_once,
        throw Exception(ErrorCodes::KEEPER_EXCEPTION, "Injected failure while storing shared scalar '{}'", name););

    auto component_guard = Coordination::setCurrentComponent("NamedScalarDefinitionStoreShared::publishDefinition");
    auto zookeeper = getZooKeeper();
    const auto def_path = definitionPath(name);

    Coordination::Stat def_stat;
    String existing_def;
    const bool def_exists = zookeeper->tryGet(def_path, existing_def, &def_stat);

    if (def_exists && !or_replace)
    {
        if (if_not_exists)
            return false;
        throw Exception(ErrorCodes::NAMED_SCALAR_ALREADY_EXISTS, "Shared scalar '{}' already exists", name);
    }

    if (def_exists)
    {
        const auto code = zookeeper->trySet(def_path, definition_blob, def_stat.version);
        if (code == Coordination::Error::ZBADVERSION)
            throw Exception(
                ErrorCodes::NAMED_SCALAR_ALREADY_EXISTS,
                "Shared scalar '{}' was modified concurrently by another node; retry the OR REPLACE",
                name);
        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException::fromPath(code, def_path);
        return true;
    }

    const auto code = zookeeper->tryCreate(def_path, definition_blob, zkutil::CreateMode::Persistent);
    if (code == Coordination::Error::ZNODEEXISTS && if_not_exists)
        return false;
    if (code == Coordination::Error::ZNODEEXISTS)
        throw Exception(ErrorCodes::NAMED_SCALAR_ALREADY_EXISTS, "Shared scalar '{}' already exists", name);
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, def_path);

    if (!zookeeper->exists(def_path))
        throw Exception(ErrorCodes::KEEPER_EXCEPTION, "Definition for shared scalar '{}' disappeared after successful publish", name);
    return true;
}

std::vector<LoadedNamedScalarDefinition> NamedScalarDefinitionStoreShared::loadAll()
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarDefinitionStoreShared::loadAll");
    auto zookeeper = getZooKeeper();
    Strings children = zookeeper->getChildren(definitionsRootPath());

    std::vector<LoadedNamedScalarDefinition> definitions;
    definitions.reserve(children.size());

    for (const auto & escaped : children)
    {
        auto name = unescapeForFileName(escaped);
        if (name.empty())
            continue;

        String blob;
        if (zookeeper->tryGet(definitionPath(name), blob))
            definitions.push_back({std::move(name), std::move(blob)});
    }

    return definitions;
}

Strings NamedScalarDefinitionStoreShared::listDefinitionsWithChildrenWatch(std::function<void()> on_change)
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarDefinitionStoreShared::listDefinitionsWithChildrenWatch");
    auto zookeeper = getZooKeeper();
    auto watcher = zookeeper->createWatchFromRawCallback(
        fmt::format("SharedNamedScalarsWatcher(named_scalars/)"),
        [cb = std::move(on_change)]() -> Coordination::WatchCallback
        {
            return [cb](const Coordination::WatchResponse &) { cb(); };
        });

    Coordination::Stat stat;
    Strings children = zookeeper->getChildrenWatch(definitionsRootPath(), &stat, watcher);

    Strings names;
    names.reserve(children.size());
    for (const auto & escaped : children)
    {
        auto n = unescapeForFileName(escaped);
        if (!n.empty())
            names.push_back(std::move(n));
    }
    return names;
}

bool NamedScalarDefinitionStoreShared::readDefinition(const String & name, String & out)
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarDefinitionStoreShared::readDefinition");
    auto zookeeper = getZooKeeper();
    return zookeeper->tryGet(definitionPath(name), out);
}

bool NamedScalarDefinitionStoreShared::readDefinitionWithDataWatch(
    const String & name,
    String & out,
    std::function<void()> on_change)
{
    auto component_guard = Coordination::setCurrentComponent("NamedScalarDefinitionStoreShared::readDefinitionWithDataWatch");
    auto zookeeper = getZooKeeper();
    auto watcher = zookeeper->createWatchFromRawCallback(
        fmt::format("NamedScalar(definition/{})", name),
        [cb = std::move(on_change)]() -> Coordination::WatchCallback
        {
            return [cb](const Coordination::WatchResponse &) { cb(); };
        });
    Coordination::Stat stat;
    return zookeeper->tryGetWatch(definitionPath(name), out, &stat, watcher);
}

}
