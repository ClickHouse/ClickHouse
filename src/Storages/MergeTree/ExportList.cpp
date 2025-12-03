#include <Storages/MergeTree/ExportList.h>

namespace DB
{

ExportsListElement::ExportsListElement(
    const StorageID & source_table_id_,
    const StorageID & destination_table_id_,
    UInt64 part_size_,
    const String & part_name_,
    const String & target_file_name_,
    UInt64 total_rows_to_read_,
    UInt64 total_size_bytes_compressed_,
    UInt64 total_size_bytes_uncompressed_,
    time_t create_time_,
    const ContextPtr & context)
: source_table_id(source_table_id_)
, destination_table_id(destination_table_id_)
, part_size(part_size_)
, part_name(part_name_)
, destination_file_path(target_file_name_)
, total_rows_to_read(total_rows_to_read_)
, total_size_bytes_compressed(total_size_bytes_compressed_)
, total_size_bytes_uncompressed(total_size_bytes_uncompressed_)
, create_time(create_time_)
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
    res.destination_file_path = destination_file_path;
    res.rows_read = rows_read;
    res.total_rows_to_read = total_rows_to_read;
    res.total_size_bytes_compressed = total_size_bytes_compressed;
    res.total_size_bytes_uncompressed = total_size_bytes_uncompressed;
    res.bytes_read_uncompressed = bytes_read_uncompressed;
    res.memory_usage = getMemoryUsage();
    res.peak_memory_usage = getPeakMemoryUsage();
    res.create_time = create_time;
    res.elapsed = elapsed;
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
