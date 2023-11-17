#include "DiskObjectStorageVFS.h"
#include "ObjectStorageVFSGCThread.h"
#include "DiskObjectStorageVFSTransaction.h"
#include "Interpreters/Context.h"

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
    , zookeeper(std::move(zookeeper_))
{
    zookeeper->createAncestors(VFS_LOG_ITEM);
}

DiskObjectStorageVFS::~DiskObjectStorageVFS() = default;

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

DiskTransactionPtr DiskObjectStorageVFS::createObjectStorageTransaction()
{
    return std::make_shared<DiskObjectStorageVFSTransaction>(*object_storage, *metadata_storage, zookeeper);
}

std::unique_ptr<ReadBufferFromFileBase> DiskObjectStorageVFS::readObject(const StoredObject& object)
{
    return object_storage->readObject(object);
}

void DiskObjectStorageVFS::removeObjects(StoredObjects && objects)
{
    object_storage->removeObjects(objects);
}
}
