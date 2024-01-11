#include "DiskObjectStorageVFS.h"
#include "DiskObjectStorageVFSTransaction.h"
#include "IO/S3Common.h"
#include "Interpreters/Context.h"
#include "ObjectStorageVFSGCThread.h"

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

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
    if (send_metadata)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "VFS doesn't support send_metadata");
    log = &Poco::Logger::get(fmt::format("DiskVFS({})", log_name));
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

    do // TODO myrrc some stop condition e.g. when we are shutting down
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

bool DiskObjectStorageVFS::tryDownloadMetadata(std::string_view remote_from, const String & to)
{
    auto buf = object_storage->readObject(getMetadataObject(remote_from));
    auto tx = metadata_storage->createTransaction();
    tx->createDirectoryRecursive(to);

    try
    {
        buf->eof(); // TODO myrrc do not write error message with "Information" level as it's a normal situation
    }
#if USE_AWS_S3
    catch (const S3Exception & e) // TODO myrrc this works only for s3
    {
        if (e.getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY)
            return false;
        throw;
    }
#endif
    catch (...)
    {
        throw;
    }

    String str;
    while (!buf->eof())
    {
        str.clear();
        readNullTerminated(str, *buf);
        const fs::path full_path = fs::path(to) / str;
        LOG_TRACE(log, "Metadata: downloading {} to {}", str, full_path);
        str.clear();
        readNullTerminated(str, *buf);
        tx->writeStringToFile(full_path, str);
    }
    tx->commit();
    return true;
}

void DiskObjectStorageVFS::uploadMetadata(std::string_view remote_to, const String & from)
{
    const StoredObject metadata_object = getMetadataObject(remote_to);
    LOG_DEBUG(log, "Metadata: uploading to {}", metadata_object);

    Strings files; // iterateDirectory() is non-recursive (we need that for projections)
    for (const fs::directory_entry & entry : fs::recursive_directory_iterator(from))
        if (entry.is_regular_file())
            files.emplace_back(entry.path());

    auto buf = object_storage->writeObject(metadata_object, WriteMode::Rewrite);
    for (auto & [path, metadata] : getSerializedMetadata(files))
    {
        const String relative_path = fs::path(path).lexically_relative(from);
        LOG_TRACE(log, "Metadata: uploading {}", relative_path);
        writeNullTerminatedString(relative_path, *buf);
        writeNullTerminatedString(metadata, *buf);
    }
    buf->finalize();
}

zkutil::ZooKeeperPtr DiskObjectStorageVFS::zookeeper()
{
    // TODO myrrc support remote_fs_zero_copy_zookeeper_path
    // TODO myrrc this is incredibly slow, however, we can't cache zookeeper ptr on init
    //  as we need to change it on expiration, and a data race occurs
    // TODO myrrc what if global context instance is nullptr due to shutdown?
    return Context::getGlobalContextInstance()->getZooKeeper();
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
    // TODO myrrc this works only for S3. Must also recheck encrypted disk replication
    // We must include disk name as two disks with different names might use same object storage bucket
    String remote_key = fmt::format("vfs/_{}_{}", name, remote);
    return StoredObject{ObjectStorageKey::createAsRelative(object_key_prefix, std::move(remote_key)).serialize()};
}
}
