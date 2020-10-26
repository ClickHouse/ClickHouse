#include <Storages/MergeTree/ReplicatedFetchesList.h>
#include <Common/CurrentMetrics.h>
#include <common/getThreadId.h>
#include <Common/CurrentThread.h>

namespace DB
{
ReplicatedFetchListElement::ReplicatedFetchListElement(
    const std::string & database_, const std::string & table_,
    const std::string & partition_id_, const std::string & result_part_name_,
    const std::string & result_part_path_, const std::string & source_replica_path_,
    const std::string & source_replica_address_, const std::string & interserver_scheme_,
    UInt8 to_detached_, UInt64 total_size_bytes_compressed_)
    : database(database_)
    , table(table_)
    , partition_id(partition_id_)
    , result_part_name(result_part_name_)
    , result_part_path(result_part_path_)
    , source_replica_path(source_replica_path_)
    , source_replica_address(source_replica_address_)
    , interserver_scheme(interserver_scheme_)
    , to_detached(to_detached_)
    , total_size_bytes_compressed(total_size_bytes_compressed_)
{
    background_thread_memory_tracker = CurrentThread::getMemoryTracker();
    if (background_thread_memory_tracker)
    {
        background_thread_memory_tracker_prev_parent = background_thread_memory_tracker->getParent();
        background_thread_memory_tracker->setParent(&memory_tracker);
    }
}


ReplicatedFetchInfo ReplicatedFetchListElement::getInfo() const
{
    ReplicatedFetchInfo res;
    res.database = database;
    res.table = table;
    res.partition_id = partition_id;
    res.result_part_name = result_part_name;
    res.result_part_path = result_part_path;
    res.source_replica_path = source_replica_path;
    res.source_replica_address = source_replica_address;
    res.interserver_scheme = interserver_scheme;
    res.to_detached = to_detached;
    res.elapsed = watch.elapsedSeconds();
    res.progress = progress.load(std::memory_order_relaxed);
    res.bytes_read_compressed = bytes_read_compressed.load(std::memory_order_relaxed);
    res.total_size_bytes_compressed = total_size_bytes_compressed;
    res.memory_usage = memory_tracker.get();
    res.thread_id = thread_id;
    return res;
}

ReplicatedFetchListElement::~ReplicatedFetchListElement()
{
    /// Unplug memory_tracker from current background processing pool thread
    if (background_thread_memory_tracker)
        background_thread_memory_tracker->setParent(background_thread_memory_tracker_prev_parent);
}
  
}
