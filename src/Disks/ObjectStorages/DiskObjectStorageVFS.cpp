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
    , gc_sleep_ms(config.getUInt64(config_prefix + ".vfs_gc_sleep_ms", 10'000))
    , traits(VFSTraits{name_})
{
    log = &Poco::Logger::get(fmt::format("DiskVFS({})", log_name));
    //chassert(!send_metadata); TODO myrrc this fails in integration tests
    zookeeper()->createAncestors(traits.log_item);
}

DiskObjectStoragePtr DiskObjectStorageVFS::createDiskObjectStorage()
{
    const auto config_prefix = "storage_configuration.disks." + name;
    return std::make_shared<DiskObjectStorageVFS>(
        getName(),
        object_key_prefix,
        log->name(),
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

bool DiskObjectStorageVFS::shouldUploadMetadata(std::string_view remote) const
{
    return !object_storage->exists(getMetadataObject(remote));
}

void DiskObjectStorageVFS::uploadMetadata(std::string_view remote_to, const String & from)
{
    auto obj = getMetadataObject(remote_to);
    Strings files; // TODO myrrc does it iterate subdirs? E.g. projections
    for (auto it = iterateDirectory(from); it->isValid(); it->next())
        files.emplace_back(it->path());
    LOG_DEBUG(log, "Metadata: uploading to {}", obj);

    auto buf = object_storage->writeObject(obj, WriteMode::Rewrite);
    for (auto & [filename, metadata] : getSerializedMetadata(files))
    {
        String relative_path = fs::path(filename).lexically_relative(from);
        LOG_TRACE(log, "Metadata: uploading {}", relative_path);
        writeString(relative_path + "\n" + metadata, *buf);
    }
    buf->finalize();
}

void DiskObjectStorageVFS::downloadMetadata(std::string_view remote_from, const String & to)
{
    auto buf = object_storage->readObject(getMetadataObject(remote_from));
    auto tx = metadata_storage->createTransaction();
    tx->createDirectoryRecursive(to);

    while (!buf->eof())
    {
        String filename;
        readString(filename, *buf);
        const auto full_path = fs::path(to) / filename;
        LOG_TRACE(log, "Metadata: downloading {} to {}", filename, full_path);
        assertChar('\n', *buf);
        // TODO myrrc this works only for S3
        DiskObjectStorageMetadata md(object_key_prefix, full_path);
        md.deserialize(*buf); // TODO myrrc just write to local filesystem
        tx->writeStringToFile(full_path, md.serializeToString());
    }
    tx->commit();
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

StoredObject DiskObjectStorageVFS::getMetadataObject(std::string_view remote) const
{
    // TODO myrrc this works only for s3
    return StoredObject{ObjectStorageKey::createAsRelative(object_key_prefix, fmt::format("vfs/{}", remote)).serialize()};
}
}
