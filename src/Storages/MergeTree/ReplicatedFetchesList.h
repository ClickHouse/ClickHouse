#pragma once
#include <Common/CurrentMetrics.h>
#include <boost/noncopyable.hpp>
#include <Storages/MergeTree/BackgroundProcessList.h>
#include <Common/Stopwatch.h>
#include <Common/MemoryTracker.h>
#include <Poco/URI.h>

namespace CurrentMetrics
{
    extern const Metric ReplicatedFetch;
}

namespace DB
{

struct ReplicatedFetchInfo
{
    std::string database;
    std::string table;
    std::string partition_id;

    std::string result_part_name;
    std::string result_part_path;

    std::string source_replica_path;
    std::string source_replica_hostname;
    UInt16 source_replica_port;
    std::string interserver_scheme;
    std::string uri;

    UInt8 to_detached;

    Float64 elapsed;
    Float64 progress;

    UInt64 total_size_bytes_compressed;
    UInt64 bytes_read_compressed;

    UInt64 thread_id;
};


struct ReplicatedFetchListElement : private boost::noncopyable
{
    const std::string database;
    const std::string table;
    const std::string partition_id;

    const std::string result_part_name;
    const std::string result_part_path;

    std::string source_replica_path;
    std::string source_replica_hostname;
    UInt16 source_replica_port;
    std::string interserver_scheme;
    std::string uri;

    const UInt8 to_detached;

    Stopwatch watch;
    std::atomic<Float64> progress{};
    std::atomic<bool> is_cancelled{};
    std::atomic<UInt64> bytes_read_compressed{};
    UInt64 total_size_bytes_compressed{};

    UInt64 thread_id;

    ReplicatedFetchListElement(
        const std::string & database_, const std::string & table_,
        const std::string & partition_id_, const std::string & result_part_name_,
        const std::string & result_part_path_, const std::string & source_replica_path_,
        const Poco::URI & uri, UInt8 to_detached_, UInt64 total_size_bytes_compressed_);

    ReplicatedFetchInfo getInfo() const;
};


using ReplicatedFetchListEntry = BackgroundProcessListEntry<ReplicatedFetchListElement, ReplicatedFetchInfo>;

class ReplicatedFetchList final : public BackgroundProcessList<ReplicatedFetchListElement, ReplicatedFetchInfo>
{
private:
    using Parent = BackgroundProcessList<ReplicatedFetchListElement, ReplicatedFetchInfo>;

public:
    ReplicatedFetchList ()
        : Parent(CurrentMetrics::ReplicatedFetch)
    {}
};

}
