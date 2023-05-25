#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <Common/CurrentMetrics.h>
#include <base/getThreadId.h>

namespace DB
{

ReplicatedFetchListElement::ReplicatedFetchListElement(
    const std::string & database_, const std::string & table_,
    const std::string & partition_id_, const std::string & result_part_name_,
    const std::string & result_part_path_, const std::string & source_replica_path_,
    const Poco::URI & uri_, UInt8 to_detached_, UInt64 total_size_bytes_compressed_)
    : database(database_)
    , table(table_)
    , partition_id(partition_id_)
    , result_part_name(result_part_name_)
    , result_part_path(result_part_path_)
    , source_replica_path(source_replica_path_)
    , source_replica_hostname(uri_.getHost())
    , source_replica_port(uri_.getPort())
    , interserver_scheme(uri_.getScheme())
    , uri(uri_.toString())
    , to_detached(to_detached_)
    , total_size_bytes_compressed(total_size_bytes_compressed_)
    , thread_id(getThreadId())
{
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
    res.source_replica_hostname = source_replica_hostname;
    res.source_replica_port = source_replica_port;
    res.interserver_scheme = interserver_scheme;
    res.uri = uri;
    res.to_detached = to_detached;
    res.elapsed = watch.elapsedSeconds();
    res.progress = progress.load(std::memory_order_relaxed);
    res.bytes_read_compressed = bytes_read_compressed.load(std::memory_order_relaxed);
    res.total_size_bytes_compressed = total_size_bytes_compressed;
    res.thread_id = thread_id;
    return res;
}

}
