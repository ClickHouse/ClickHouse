#include "VFSTransactionGroup.h"
#include "DiskObjectStorageVFS.h"

namespace DB
{
/// One logical "transaction" e.g. cloning parts between replicas can involve multiple disks
/// However, allowing VFSTransactionGroup to do that may lead to data races so that's prohibited as for now.
VFSTransactionGroup::VFSTransactionGroup(DiskPtr disk_)
    : disk(disk_->isObjectStorageVFS() ? static_cast<DiskObjectStorageVFS *>(disk_.get()) : nullptr)
{
    if (!disk)
        return;
    VFSTransactionGroup * item = nullptr;
    if (disk->group.compare_exchange_strong(item, this))
        LOG_TRACE(disk->log, "TransactionGroup: set");
    else
        LOG_TRACE(disk->log, "TransactionGroup: failed to set -- other group already present");
}

VFSTransactionGroup::~VFSTransactionGroup()
{
    if (!disk)
        return;

    Coordination::Requests req;
    const Strings nodes = serialize();
    req.reserve(nodes.size());
    for (const auto & node : nodes)
        req.emplace_back(zkutil::makeCreateRequest(disk->nodes.log_item, node, zkutil::CreateMode::PersistentSequential));
    disk->zookeeper()->multi(req);

    disk->group.store(nullptr);
    LOG_TRACE(disk->log, "TransactionGroup: removed");
}
}
