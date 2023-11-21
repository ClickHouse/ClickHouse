#include "DiskObjectStorageVFS.h"
#include "DiskObjectStorageVFSTransaction.h"
#include "Interpreters/Context.h"
#include "ObjectStorageVFSGCThread.h"

namespace DB
{
DiskObjectStoragePtr DiskObjectStorageVFS::createDiskObjectStorage()
{
    const auto config_prefix = "storage_configuration.disks." + name;
    auto ptr = std::make_shared<DiskObjectStorageVFS>(
        getName(),
        object_key_prefix,
        getName(),
        metadata_storage,
        object_storage,
        Context::getGlobalContextInstance()->getConfigRef(),
        config_prefix);
    ptr->zookeeper = zookeeper;
    return ptr;
}

void DiskObjectStorageVFS::preAccessCheck(ContextPtr context)
{
    zookeeper = context->getZooKeeper();
}

void DiskObjectStorageVFS::startupImpl(ContextPtr context)
{
    DiskObjectStorage::startupImpl(context);

    zookeeper->createAncestors(VFS_LOG_ITEM);
    // TODO myrrc ugly hack to create locks root node, remove
    zookeeper->createAncestors(fs::path(VFS_LOCKS_NODE) / "dummy");

    gc_thread = std::make_unique<ObjectStorageVFSGCThread>(*this, context);
    gc_thread->start();
}

void DiskObjectStorageVFS::shutdown()
{
    DiskObjectStorage::shutdown();
    gc_thread->stop();
}

String DiskObjectStorageVFS::getStructure() const
{
    return fmt::format("DiskObjectStorageVFS-{}({})", getName(), object_storage->getName());
}

String lockPathToFullPath(std::string_view path)
{
    String lock_path{path};
    std::ranges::replace(lock_path, '/', '_');
    return fs::path(VFS_LOCKS_NODE) / lock_path;
}

bool DiskObjectStorageVFS::lock(std::string_view path, bool block)
{
    // TODO myrrc should have something better
    using enum Coordination::Error;
    const String lock_path_full = lockPathToFullPath(path);
    const auto mode = zkutil::CreateMode::Persistent;

    LOG_DEBUG(log, "Creating lock {} (zk path {}), block={}", path, lock_path_full, block);

    if (block)
    {
        zookeeper->create(lock_path_full, "", mode);
        return true;
    }

    auto code = zookeeper->tryCreate(lock_path_full, "", mode);
    if (code == ZOK)
        return true;
    if (code == ZNODEEXISTS)
        return false;
    throw Coordination::Exception(code);
}

void DiskObjectStorageVFS::unlock(std::string_view path)
{
    const String lock_path_full = lockPathToFullPath(path);
    LOG_DEBUG(log, "Removing lock {} (zk path {})", path, lock_path_full);
    zookeeper->remove(lock_path_full);
}

DiskTransactionPtr DiskObjectStorageVFS::createObjectStorageTransaction()
{
    return std::make_shared<DiskObjectStorageVFSTransaction>(*object_storage, *metadata_storage, zookeeper);
}

std::unique_ptr<ReadBufferFromFileBase> DiskObjectStorageVFS::readObject(const StoredObject & object)
{
    return object_storage->readObject(object);
}

void DiskObjectStorageVFS::removeObjects(StoredObjects && objects)
{
    object_storage->removeObjects(objects);
}
}
