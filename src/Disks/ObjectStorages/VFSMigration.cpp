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

    Coordination::Requests req;
    const Strings nodes = item.serialize();
    LOG_TRACE(disk.log, "VFSTransaction: executing {}", fmt::join(nodes, "\n"));
    req.reserve(nodes.size());
    for (const auto & node : nodes)
        req.emplace_back(zkutil::makeCreateRequest(disk.nodes.log_item, node, zkutil::CreateMode::PersistentSequential));
    disk.zookeeper()->multi(req);

    LOG_INFO(log, "Migrated disk {}", disk.getName());
}
}
