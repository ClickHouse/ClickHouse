#include <Storages/MergeTree/ExportList.h>

namespace DB
{

ExportsListElement::ExportsListElement(
    const StorageID & source_table_id_,
    const StorageID & destination_table_id_,
    UInt64 part_size_,
    const String & part_name_,
    const std::vector<String> & destination_file_paths_,
    UInt64 total_rows_to_read_,
    UInt64 total_size_bytes_compressed_,
    UInt64 total_size_bytes_uncompressed_,
    time_t create_time_,
    const String & query_id_,
    const ContextPtr & context)
: source_table_id(source_table_id_)
, destination_table_id(destination_table_id_)
, part_size(part_size_)
, part_name(part_name_)
, destination_file_paths(destination_file_paths_)
, total_rows_to_read(total_rows_to_read_)
, total_size_bytes_compressed(total_size_bytes_compressed_)
, total_size_bytes_uncompressed(total_size_bytes_uncompressed_)
, create_time(create_time_)
, query_id(query_id_)
{
    thread_group = ThreadGroup::createForMergeMutate(context);
}

ExportsListElement::~ExportsListElement()
{
    background_memory_tracker.adjustOnBackgroundTaskEnd(&thread_group->memory_tracker);
}

ExportInfo ExportsListElement::getInfo() const
{
    ExportInfo res;
    res.source_database = source_table_id.database_name;
    res.source_table = source_table_id.table_name;
    res.destination_database = destination_table_id.database_name;
    res.destination_table = destination_table_id.table_name;
    res.part_name = part_name;

    {
        std::shared_lock lock(destination_file_paths_mutex);
        res.destination_file_paths = destination_file_paths;
    }

    res.rows_read = rows_read.load(std::memory_order_relaxed);
    res.total_rows_to_read = total_rows_to_read;
    res.total_size_bytes_compressed = total_size_bytes_compressed;
    res.total_size_bytes_uncompressed = total_size_bytes_uncompressed;
    res.bytes_read_uncompressed = bytes_read_uncompressed.load(std::memory_order_relaxed);
    res.memory_usage = getMemoryUsage();
    res.peak_memory_usage = getPeakMemoryUsage();
    res.create_time = create_time;
    res.elapsed = watch.elapsedSeconds();
    res.query_id = query_id;
    return res;
}

UInt64 ExportsListElement::getMemoryUsage() const
{
    return thread_group->memory_tracker.get();
}

UInt64 ExportsListElement::getPeakMemoryUsage() const
{
    return thread_group->memory_tracker.getPeak();
}

}
