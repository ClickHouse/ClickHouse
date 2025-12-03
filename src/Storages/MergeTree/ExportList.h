#pragma once

#include <Storages/MergeTree/BackgroundProcessList.h>
#include <Interpreters/StorageID.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadStatus.h>
#include <Poco/URI.h>
#include <boost/noncopyable.hpp>

namespace CurrentMetrics
{
    extern const Metric Export;
}

namespace DB
{

struct ExportInfo
{
    String source_database;
    String source_table;
    String destination_database;
    String destination_table;
    String part_name;
    String destination_file_path;
    UInt64 rows_read;
    UInt64 total_rows_to_read;
    UInt64 total_size_bytes_compressed;
    UInt64 total_size_bytes_uncompressed;
    UInt64 bytes_read_uncompressed;
    UInt64 memory_usage;
    UInt64 peak_memory_usage;
    time_t create_time = 0;
    Float64 elapsed;
};

struct ExportsListElement : private boost::noncopyable
{
    const StorageID source_table_id;
    const StorageID destination_table_id;
    const UInt64 part_size;
    const String part_name;
    String destination_file_path;
    UInt64 rows_read {0};
    UInt64 total_rows_to_read {0};
    UInt64 total_size_bytes_compressed {0};
    UInt64 total_size_bytes_uncompressed {0};
    UInt64 bytes_read_uncompressed {0};
    time_t create_time {0};
    Float64 elapsed {0};

    Stopwatch watch;
    ThreadGroupPtr thread_group;

    ExportsListElement(
        const StorageID & source_table_id_,
        const StorageID & destination_table_id_,
        UInt64 part_size_,
        const String & part_name_,
        const String & destination_file_path_,
        UInt64 total_rows_to_read_,
        UInt64 total_size_bytes_compressed_,
        UInt64 total_size_bytes_uncompressed_,
        time_t create_time_,
        const ContextPtr & context);

    ~ExportsListElement();

    ExportInfo getInfo() const;

    UInt64 getMemoryUsage() const;
    UInt64 getPeakMemoryUsage() const;
};


class ExportsList final : public BackgroundProcessList<ExportsListElement, ExportInfo>
{
private:
    using Parent = BackgroundProcessList<ExportsListElement, ExportInfo>;

public:
    ExportsList()
        : Parent(CurrentMetrics::Export)
    {}
};

using ExportsListEntry = BackgroundProcessListEntry<ExportsListElement, ExportInfo>;

}
