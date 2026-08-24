#include <memory>
#include <Access/AccessEntityIO.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/ReplicatedAccessStorage.h>
#include <Access/AccessChangesNotifier.h>
#include <Access/AccessBackup.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <base/range.h>
#include <base/sleep.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ReplicatedAccessStorage::ReplicatedAccessStorage(
    const String & storage_name_,
    const String & zookeeper_path_,
    zkutil::GetZooKeeper get_zookeeper_,
    AccessChangesNotifier & changes_notifier_,
    bool allow_backup_)
    : IAccessStorage(storage_name_)
    , memory_storage(storage_name_, changes_notifier_, false)
    , replicator(storage_name_, zookeeper_path_, get_zookeeper_, changes_notifier_, memory_storage)
    , backup_allowed(allow_backup_)
{
    if (zookeeper_path_.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path must be non-empty");
}

ReplicatedAccessStorage::~ReplicatedAccessStorage()
{
    try
    {
        ReplicatedAccessStorage::shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ReplicatedAccessStorage::shutdown()
{
    replicator.shutdown();
}

template <typename Func>
static void retryOnZooKeeperUserError(size_t attempts, Func && function)
{
    while (attempts > 0)
    {
        try
        {
            function();
            return;
        }
        catch (zkutil::KeeperException & keeper_exception)
        {
            if (Coordination::isUserError(keeper_exception.code) && attempts > 1)
                attempts -= 1;
            else
                throw;
        }
    }
}

bool ReplicatedAccessStorage::insertImpl(const UUID & id, const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists, UUID * conflicting_id)
{
    return replicator.insertEntity(id, new_entity, replace_if_exists, throw_if_exists, conflicting_id);
}

bool ReplicatedAccessStorage::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    return replicator.removeEntity(id, throw_if_not_exists);
}

bool ReplicatedAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    return replicator.updateEntity(id, update_func, throw_if_not_exists);
}

void ReplicatedAccessStorage::reload(ReloadMode reload_mode)
{
    replicator.reload(reload_mode == ReloadMode::ALL);
}

std::optional<UUID> ReplicatedAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    return replicator.findEntity(type, name);
}

std::vector<UUID> ReplicatedAccessStorage::findAllImpl(AccessEntityType type) const
{
    return replicator.findAllEntities(type);
}

bool ReplicatedAccessStorage::exists(const UUID & id) const
{
    return replicator.exists(id);
}

AccessEntityPtr ReplicatedAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    return replicator.readEntity(id, throw_if_not_exists);
}

}
