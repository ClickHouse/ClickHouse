#include "VFSTransactionGroup.h"
#include "DiskObjectStorageVFS.h"

namespace DB
{
VFSTransactionGroup::VFSTransactionGroup(DiskPtr disk_)
    : disk(disk_->isObjectStorageVFS() ? static_cast<DiskObjectStorageVFS *>(disk_.get()) : nullptr)
{
    if (!disk)
        return;
    if (disk->tryAddGroup(this))
        LOG_TRACE(disk->log, "TransactionGroup: set");
    else
    {
        LOG_TRACE(disk->log, "TransactionGroup: failed to set");
        disk = nullptr;
    }
}

VFSTransactionGroup::~VFSTransactionGroup()
{
    if (!disk || std::uncaught_exceptions())
        return; // Don't commit on stack unwinding

    Coordination::Requests req;
    const Strings nodes = serialize();
    LOG_TRACE(disk->log, "TransactionGroup: executing {}", fmt::join(nodes, "\n"));
    req.reserve(nodes.size());
    for (const auto & node : nodes)
        req.emplace_back(zkutil::makeCreateRequest(disk->nodes.log_item, node, zkutil::CreateMode::PersistentSequential));
    disk->zookeeper()->multi(req);

    disk->removeGroup();
    LOG_TRACE(disk->log, "TransactionGroup: removed");
}
}
