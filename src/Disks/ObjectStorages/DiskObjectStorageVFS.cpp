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
    zkutil::ZooKeeperPtr zookeeper_)
    : DiskObjectStorage( //NOLINT
        name_,
        object_storage_root_path_,
        log_name,
        std::move(metadata_storage_),
        std::move(object_storage_),
        config,
        config_prefix)
    , traits(name_)
    , zookeeper(std::move(zookeeper_))
{
    zookeeper->createAncestors(traits.VFS_LOG_ITEM);
    // TODO myrrc ugly hack to create locks root node, remove
    zookeeper->createAncestors(fs::path(traits.VFS_LOCKS_NODE) / "dummy");
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
        zookeeper);
}

void DiskObjectStorageVFS::startupImpl(ContextPtr context)
{
    DiskObjectStorage::startupImpl(context);
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

String DiskObjectStorageVFS::lockPathToFullPath(std::string_view path)
{
    String lock_path{path};
    std::ranges::replace(lock_path, '/', '_');
    return fs::path(traits.VFS_LOCKS_NODE) / lock_path;
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
        // TODO myrrc what if wait returns but create() fails?
        zookeeper->waitForDisappear(lock_path_full);
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

void DiskObjectStorageVFS::copyFileReverse(
    const String & from_file_path,
    IDisk & from_disk,
    const String & to_file_path,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const std::function<void()> & cancellation_hook)
{
    if (const String & metadata = gc_thread->findInLog(to_file_path); !metadata.empty())
    {
        LOG_DEBUG(log, "Found metadata for {} in log, propagating to local filesystem", to_file_path);
        auto buf = WriteBufferFromFile{to_file_path};
        writeString(metadata, buf);
    }

    LOG_DEBUG(log, "No metadata found for {}, copying", to_file_path);
    IDisk::copyFileReverse(from_file_path, from_disk, to_file_path, read_settings, write_settings, cancellation_hook);
}

DiskTransactionPtr DiskObjectStorageVFS::createObjectStorageTransaction()
{
    return std::make_shared<DiskObjectStorageVFSTransaction>(*object_storage, *metadata_storage, zookeeper, traits);
}
}
