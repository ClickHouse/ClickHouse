#include "VFSMigration.h"
#include "DiskObjectStorageVFS.h"

namespace DB
{
VFSMigration::VFSMigration(DiskObjectStorageVFS & disk_, ContextWeakPtr ctx)
    : WithContext(std::move(ctx)), disk(disk_), log(getLogger(fmt::format("VFSMigration({})", disk.getName())))
{
}

void VFSMigration::migrate() const
{
    LOG_INFO(log, "Migrating disk {}", disk.getName());
    auto ctx = getContext();

    MetadataStoragePtr metadata_storage = disk.getMetadataStorage();

    // TODO myrrc batch in 1MB sizes (may be done on VFSTransaction side by @mkmkme)
    VFSLogItem item;

    const auto path = fs::path(disk.getPath());
    LOG_DEBUG(log, "Will iterate {}", path);

    // TODO myrrc rewrite using Common/AsyncLoader.h
    for (const auto & entry : fs::recursive_directory_iterator{path})
        if (entry.is_regular_file())
            for (const StoredObject & elem : metadata_storage->getStorageObjects(entry.path()))
            {
                const size_t links = fs::hard_link_count(entry);
                if (auto it = item.find(elem.remote_path); it == item.end())
                    item.emplace(elem.remote_path, links);
                else
                    it->second += links;
            }

    // TODO myrrc write to s3 instead
    disk.zookeeper()->create(disk.nodes.log_item, item.serialize(), zkutil::CreateMode::PersistentSequential);

    LOG_INFO(log, "Migrated disk {}", disk.getName());
}
}
