#include "VFSTransactionGroup.h"
#include "DiskObjectStorageVFS.h"

namespace DB
{
// Transaction group must only capture operations on the thread the object was created
// to allow multiple concurrent operations (e.g. part loading and merge) to have different groupers
// We must also allow thread operating on multiple disks to have multiple groupers
static thread_local DiskObjectStorageVFS * current_disk {nullptr};

VFSTransactionGroup::VFSTransactionGroup(DiskPtr disk_)
    : disk(disk_->isObjectStorageVFS() ? static_cast<DiskObjectStorageVFS *>(disk_.get()) : nullptr)
{
    if (!disk)
        return;
    const bool first = disk->group.compare_exchange_strong(parent, this);
    LOG_TRACE(disk->log, "TransactionGroup: {}", first ? "set" : "attached to parent");
}

VFSTransactionGroup::~VFSTransactionGroup()
{
    if (disk && std::uncaught_exceptions() == 0)
        flush(); // If we're unwinding stack we don't want to commit anything
}

void VFSTransactionGroup::flush()
{
    Coordination::Requests req;
    const Strings nodes = serialize();
    LOG_TRACE(disk->log, "TransactionGroup: executing {}", fmt::join(nodes, "\n"));

    req.reserve(nodes.size());
    for (const auto & node : nodes)
        req.emplace_back(zkutil::makeCreateRequest(disk->nodes.log_item, node, zkutil::CreateMode::PersistentSequential));
    disk->zookeeper()->multi(req);

    if (!parent)
        disk->group.store(nullptr);
    LOG_TRACE(disk->log, "TransactionGroup: removed");
}
}
