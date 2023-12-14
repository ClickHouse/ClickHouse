#include "DiskObjectStorageVFS.h"
#include "DiskObjectStorageVFSTransaction.h"
#include "IO/WriteBufferFromFile.h"
#include "Interpreters/Context.h"
#include "ObjectStorageVFSGCThread.h"

namespace DB
{
DiskObjectStorageVFS::DiskObjectStorageVFS(
    const String & name_,
    const String & object_storage_root_path_,
    const String & log_name,
    MetadataStoragePtr metadata_storage_,
    ObjectStoragePtr object_storage_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    zkutil::ZooKeeperPtr zookeeper_,
    bool allow_gc_)
    : DiskObjectStorage( //NOLINT
        name_,
        object_storage_root_path_,
        log_name,
        std::move(metadata_storage_),
        std::move(object_storage_),
        config,
        config_prefix)
    , allow_gc(allow_gc_)
    , gc_thread_sleep_ms(config.getUInt64(config_prefix + ".object_storage_vfs_gc_period", 10'000))
    , traits(VFSTraits{name_})
    , zookeeper(std::move(zookeeper_))
{
    zookeeper->createAncestors(traits.log_item);
    // TODO myrrc ugly hack to create traits node, remove
    zookeeper->createAncestors(fs::path(traits.locks_node) / "dummy");
}

DiskObjectStoragePtr DiskObjectStorageVFS::createDiskObjectStorage()
{
    const auto config_prefix = "storage_configuration.disks." + name;
    return std::make_shared<DiskObjectStorageVFS>(
        getName(),
        object_key_prefix,
        getName(),
        metadata_storage,
        object_storage,
        Context::getGlobalContextInstance()->getConfigRef(),
        config_prefix,
        zookeeper,
        allow_gc);
}

void DiskObjectStorageVFS::startupImpl(ContextPtr context)
{
    DiskObjectStorage::startupImpl(context);
    if (!allow_gc)
        return;
    garbage_collector.emplace(*this, context);
}

void DiskObjectStorageVFS::shutdown()
{
    DiskObjectStorage::shutdown();
    if (garbage_collector)
        garbage_collector->stop();
}

String DiskObjectStorageVFS::getStructure() const
{
    return fmt::format("DiskObjectStorageVFS-{}({})", getName(), object_storage->getName());
}

bool DiskObjectStorageVFS::lock(std::string_view path, bool block)
{
    using enum Coordination::Error;
    const String lock_path_full = lockPathToFullPath(path);
    const auto mode = zkutil::CreateMode::Ephemeral;

    LOG_TRACE(log, "Creating lock {} (zk path {}), block={}", path, lock_path_full, block);

    // TODO myrrc need something better for blocking case as now we should nearly always lose in tryCreate
    // in case there's contention on node
    do
    {
        if (block)
            zookeeper->waitForDisappear(lock_path_full);
        const auto code = zookeeper->tryCreate(lock_path_full, "", mode);
        if (code == ZOK)
            return true;
        if (code == ZNODEEXISTS && !block)
            return false;
        if (code != ZNODEEXISTS)
            throw Coordination::Exception(code, "While trying to create lock {}", code);
    } while (true);
}

void DiskObjectStorageVFS::unlock(std::string_view path)
{
    const String lock_path_full = lockPathToFullPath(path);
    LOG_TRACE(log, "Removing lock {} (zk path {})", path, lock_path_full);
    zookeeper->remove(lock_path_full);
}

DiskTransactionPtr DiskObjectStorageVFS::createObjectStorageTransaction()
{
    return std::make_shared<DiskObjectStorageVFSTransaction>(*object_storage, *metadata_storage, zookeeper, traits);
}

String DiskObjectStorageVFS::lockPathToFullPath(std::string_view path) const
{
    String lock_path{path};
    std::ranges::replace(lock_path, '/', '_');
    return fs::path(traits.locks_node) / lock_path;
}
}
