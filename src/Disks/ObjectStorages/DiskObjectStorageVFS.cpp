#include "DiskObjectStorageVFS.h"

#include <Disks/ObjectStorages/DiskObjectStorageVFSTransaction.h>
#include <Disks/ObjectStorages/VFSGarbageCollector.h>
#include <Disks/ObjectStorages/VFSMigration.h>
#include <IO/S3Common.h>
#include <Interpreters/Context.h>
#if USE_AZURE_BLOB_STORAGE
#    include <azure/storage/common/storage_exception.hpp>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

DiskObjectStorageVFS::DiskObjectStorageVFS(
    const String & name_,
    const String & object_key_prefix_,
    MetadataStoragePtr metadata_storage_,
    ObjectStoragePtr object_storage_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    bool enable_gc_)
    : DiskObjectStorage(name_, object_key_prefix_, std::move(metadata_storage_), std::move(object_storage_), config, config_prefix)
    , enable_gc(enable_gc_)
    , object_storage_type(object_storage->getType())
    , nodes(VFSNodes{name}) // TODO myrrc disk_vfs_id = disk_name implies disk name consistency across replicas
    , settings(MultiVersion<VFSSettings>{std::make_unique<VFSSettings>(config, config_prefix)})
{
    if (object_storage_type != ObjectStorageType::S3 && object_storage_type != ObjectStorageType::Azure)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "VFS supports 's3' or 'azure_blob_storage' disk type");
    if (send_metadata)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "VFS doesn't support send_metadata");
    zookeeper()->createAncestors(nodes.log_item);
    snapshot_storage = std::make_unique<VFSSnapshotObjectStorage>(object_storage, makeSnapshotStoragePrefix(), *settings.get());

    log = getLogger(fmt::format("DiskVFS({})", name));
}

String DiskObjectStorageVFS::makeSnapshotStoragePrefix() const
{
    String res = fmt::format("vfs/{}/snapshots/", name);
    // Azure blob storage does not work correctly with slashes in names
    if (object_storage_type == ObjectStorageType::Azure)
        std::ranges::replace(res, '/', '_');
    return object_key_prefix + res;
}

DiskObjectStoragePtr DiskObjectStorageVFS::createDiskObjectStorage()
{
    const auto config_prefix = "storage_configuration.disks." + name;
    return std::make_shared<DiskObjectStorageVFS>(
        getName(),
        object_key_prefix,
        metadata_storage,
        object_storage,
        Context::getGlobalContextInstance()->getConfigRef(),
        config_prefix,
        enable_gc);
}

void DiskObjectStorageVFS::startupImpl(ContextPtr context)
{
    DiskObjectStorage::startupImpl(context);

    const auto & ctx_settings = context->getSettingsRef();
    auto new_settings = std::make_unique<VFSSettings>(*settings.get());
    new_settings->keeper_fault_injection_probability = ctx_settings.insert_keeper_fault_injection_probability;
    new_settings->keeper_fault_injection_seed = ctx_settings.insert_keeper_fault_injection_seed;
    LOG_INFO(log, "VFS settings: {}, GC enabled: {}", *new_settings, enable_gc);
    settings.set(std::move(new_settings));

    if (!enable_gc)
        return;

    // Assuming you can't migrate while having GC turned off
    // TODO myrrc we can't rely on user setting this correctly all the time.
    // We must find a better way to determine whether this disk
    // - Was VFS or non-VFS on start
    // - If non VFS, whether this disk was migrated last time
    // - Whether this disk needs migration (or we have to throw an exception)
    // This change must be persistent, so after successful migration we must reset the setting
    // and prevent user from setting it again.
    const String migrate_key = fmt::format("storage_configuration.disks.{}.vfs_migrate", name);
    if (context->getConfigRef().getBool(migrate_key, false))
        VFSMigration{*this, context}.migrate();

    garbage_collector.emplace(*this, context->getSchedulePool());
}

void DiskObjectStorageVFS::shutdown()
{
    DiskObjectStorage::shutdown();
    if (garbage_collector)
        garbage_collector->stop();
}

void DiskObjectStorageVFS::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String &, const DisksMap & disk_map)
{
    const auto config_prefix = "storage_configuration.disks." + name;
    const auto & ctx_settings = context->getSettingsRef();

    auto new_settings = std::make_unique<VFSSettings>(
        config, config_prefix, ctx_settings.insert_keeper_fault_injection_probability, ctx_settings.insert_keeper_fault_injection_seed);
    LOG_DEBUG(log, "New VFS settings: {}", *new_settings);
    settings.set(std::move(new_settings));

    DiskObjectStorage::applyNewSettings(config, context, config_prefix, disk_map);
}

String DiskObjectStorageVFS::getStructure() const
{
    return fmt::format("DiskObjectStorageVFS-{}({})", getName(), object_storage->getName());
}

// We want to have transaction group only for the thread it was created in to allow
// multiple concurrent tasks involving one disk (e.g. downloading part and merge).
constexpr size_t MAX_CONCURRENT_DISKS = 32;
static thread_local std::pair<DiskObjectStorageVFS *, VFSTransactionGroup *> disks_transactions[MAX_CONCURRENT_DISKS]
    = {{nullptr, nullptr}};

bool DiskObjectStorageVFS::tryAddGroup(VFSTransactionGroup * group)
{
    for (auto & pair : disks_transactions)
        if (pair.first == this)
        {
            if (pair.second)
                return false;
            pair.second = group;
            return true;
        }
    for (auto & pair : disks_transactions)
        if (!pair.first)
        {
            pair = {this, group};
            return true;
        }
    return false;
}

void DiskObjectStorageVFS::removeGroup()
{
    for (auto & pair : disks_transactions)
        if (pair.first == this)
            pair.first = nullptr;
}

VFSTransactionGroup * DiskObjectStorageVFS::getGroup() const
{
    for (auto & [disk, group] : disks_transactions)
        if (disk == this)
            return group;
    return nullptr;
}

bool DiskObjectStorageVFS::tryDownloadMetadata(std::string_view remote_from, const String & to)
{
    auto buf = object_storage->readObject(getMetadataObject(remote_from));
    auto tx = metadata_storage->createTransaction();
    tx->createDirectoryRecursive(to);

    try
    {
        buf->eof();
    }
    // TODO myrrc this works only for s3 and azure
#if USE_AWS_S3
    catch (const S3Exception & e)
    {
        if (e.getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY)
            return false;
        throw;
    }
#endif
#if USE_AZURE_BLOB_STORAGE
    catch (const Azure::Storage::StorageException & e)
    {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
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

ZooKeeperWithFaultInjectionPtr DiskObjectStorageVFS::zookeeper() const
{
    const auto settings_ref = settings.get();
    return ZooKeeperWithFaultInjection::createInstance(
        settings_ref->keeper_fault_injection_probability,
        settings_ref->keeper_fault_injection_seed,
        // TODO myrrc what if global context instance is nullptr due to shutdown?
        Context::getGlobalContextInstance()->getZooKeeper(),
        "DiskObjectStorageVFS",
        log);
}

DiskTransactionPtr DiskObjectStorageVFS::createObjectStorageTransaction()
{
    return std::make_shared<DiskObjectStorageVFSTransaction>(*this);
}

DiskTransactionPtr DiskObjectStorageVFS::createObjectStorageTransactionToAnotherDisk(DiskObjectStorage & to_disk)
{
    return std::make_shared<MultipleDisksObjectStorageVFSTransaction>(*this, *to_disk.getObjectStorage());
}

StoredObject DiskObjectStorageVFS::getMetadataObject(std::string_view remote) const
{
    // TODO myrrc this works only for S3 and Azure.
    // We must include disk name as two disks with different names might use same object storage bucket
    // vfs/ prefix is required for AWS:
    //  https://aws.amazon.com/premiumsupport/knowledge-center/s3-object-key-naming-pattern/
    String remote_key = fmt::format("vfs/{}{}", name, remote);
    return StoredObject{ObjectStorageKey::createAsRelative(object_key_prefix, std::move(remote_key)).serialize()};
}
}
