#include "DiskObjectStorageVFS.h"
#include <Interpreters/Context.h>
#include "VFSGarbageCollector.h"
#include "VFSMigration.h"
#include "VFSTransaction.h"

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
    , VFSMetadataStorage(*this)
    , enable_gc(enable_gc_)
    , nodes(VFSNodes{name}) // TODO myrrc disk_vfs_id = disk_name implies disk name consistency across replicas
    , settings(MultiVersion<VFSSettings>{std::make_unique<VFSSettings>(config, config_prefix)})
{
    const ObjectStorageType type = object_storage->getType();
    if (type != ObjectStorageType::S3 && type != ObjectStorageType::Azure)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "VFS supports 's3' or 'azure_blob_storage' disk type");
    if (send_metadata)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "VFS doesn't support send_metadata");

    zookeeper()->createAncestors(nodes.log_item);
    log = getLogger(fmt::format("DiskVFS({})", name));
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
    if (garbage_collector)
        garbage_collector->stop();
    DiskObjectStorage::shutdown();
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

ZooKeeperWithFaultInjectionPtr DiskObjectStorageVFS::zookeeper() const
{
    const auto settings_ref = settings.get();
    return ZooKeeperWithFaultInjection::createInstance(
        settings_ref->keeper_fault_injection_probability,
        settings_ref->keeper_fault_injection_seed,
        Context::getGlobalContextInstance()->getZooKeeper(),
        "DiskObjectStorageVFS",
        log);
}

DiskTransactionPtr DiskObjectStorageVFS::createObjectStorageTransaction()
{
    return std::make_shared<VFSTransaction>(*this);
}

DiskTransactionPtr DiskObjectStorageVFS::createObjectStorageTransactionToAnotherDisk(DiskObjectStorage & to_disk)
{
    return std::make_shared<MultipleDisksVFSTransaction>(*this, *to_disk.getObjectStorage());
}
}
