#include "DiskObjectStorageVFS.h"
#include "DiskObjectStorageMetadata.h"
#include "DiskObjectStorageVFSTransaction.h"
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
    bool enable_gc_)
    : DiskObjectStorage( //
        name_,
        object_storage_root_path_,
        log_name,
        std::move(metadata_storage_),
        std::move(object_storage_),
        config,
        config_prefix)
    , enable_gc(enable_gc_)
    // TODO myrrc add to docs
    , gc_sleep_ms(config.getUInt64(config_prefix + ".object_storage_vfs_gc_period", 10'000))
    , traits(VFSTraits{name_})
{
    //chassert(!send_metadata); TODO myrrc this fails in integration tests
    zookeeper()->createAncestors(traits.log_item);
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
        enable_gc);
}

void DiskObjectStorageVFS::startupImpl(ContextPtr context)
{
    DiskObjectStorage::startupImpl(context);
    if (!enable_gc)
        return;
    garbage_collector.emplace(*this, context->getSchedulePool());
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
            zookeeper()->waitForDisappear(lock_path_full);
        const auto code = zookeeper()->tryCreate(lock_path_full, "", mode);
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
    zookeeper()->remove(lock_path_full);
}

bool DiskObjectStorageVFS::moveOrLoadMetadata(const String & lock_prefix, const String & path)
{
    auto obj = StoredObject{fmt::format("data/vfs/{}", lock_prefix)};

    if (!object_storage->exists(obj))
    {
        // First replica, TODO move object to object storage
        Strings files;
        listFiles(path, files);
        auto buf = object_storage->writeObject(obj, WriteMode::Rewrite);

        for (auto & [filename, metadata] : getSerializedMetadata(files))
            writeString(filename + "\n" + metadata, *buf);

        return true;
    }

    auto buf = object_storage->readObject(obj); // Other replica finished loading part to object storage
    while (!buf->eof())
    {
        String filename;
        readString(filename, *buf);
        DiskObjectStorageMetadata md(metadata_storage->compatible_key_prefix, path + filename);
        md.deserialize(*buf); // TODO myrrc just write to local filesystem
        auto tx = metadata_storage->createTransaction();
        tx->writeStringToFile(fs::path(path) / filename, md.serializeToString());
        tx->commit();
    }

    return true;
}

zkutil::ZooKeeperPtr DiskObjectStorageVFS::zookeeper()
{
    // TODO myrrc support remote_fs_zero_copy_zookeeper_path
    if (!cached_zookeeper || cached_zookeeper->expired()) [[unlikely]]
        cached_zookeeper = Context::getGlobalContextInstance()->getZooKeeper();
    return cached_zookeeper;
}

DiskTransactionPtr DiskObjectStorageVFS::createObjectStorageTransaction()
{
    return std::make_shared<DiskObjectStorageVFSTransaction>(*this);
}

String DiskObjectStorageVFS::lockPathToFullPath(std::string_view path) const
{
    String lock_path{path};
    std::ranges::replace(lock_path, '/', '_');
    return fs::path(traits.locks_node) / lock_path;
}
}
