#include <Storages/MergeTree/ReplicatedMergeTreeQueueSizeThread.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Poco/Timestamp.h>
#include <Common/ZooKeeper/KeeperException.h>


namespace DB
{

ReplicatedMergeTreeQueueSizeThread::ReplicatedMergeTreeQueueSizeThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getFullTableName() + " (ReplicatedMergeTreeQueueSizeThread)")
    , log(&Poco::Logger::get(log_name))
{
    task = storage.getContext()->getSchedulePool().createTask(log_name, [this]{ run(); });
}

void ReplicatedMergeTreeQueueSizeThread::run()
{
    try
    {
        iterate();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    task->scheduleAfter(1000);
}


void ReplicatedMergeTreeQueueSizeThread::iterate()
{
    storage.updateMaxReplicasQueueSize();
}

}
