#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>

#include <Common/CurrentThread.h>

namespace DB
{

Block PartLogElement::createBlock()
{
    auto event_type_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"NewPart",       static_cast<Int8>(NEW_PART)},
            {"MergeParts",    static_cast<Int8>(MERGE_PARTS)},
            {"DownloadPart",  static_cast<Int8>(DOWNLOAD_PART)},
            {"RemovePart",    static_cast<Int8>(REMOVE_PART)},
            {"MutatePart",    static_cast<Int8>(MUTATE_PART)},
            {"MovePart",      static_cast<Int8>(MOVE_PART)},
        }
    );

    ColumnsWithTypeAndName columns_with_type_and_name;

    return {

        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeString>(), "query_id"),
        columns_with_type_and_name.emplace_back(std::move(event_type_datatype), "event_type"),
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeDate>(), "event_date"),

        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeDateTime>(), "event_time"),
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeDateTime64>(6), "event_time_microseconds"),

        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt64>(), "duration_ms"),

        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeString>(), "database"),
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeString>(), "table"),
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeString>(), "part_name"),
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeString>(), "partition_id"),
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeString>(), "path_on_disk"),

        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt64>(), "rows"),
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt64>(), "size_in_bytes"), // On disk

        /// Merge-specific info
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "merged_from"),
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt64>(), "bytes_uncompressed"), // Result bytes
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt64>(), "read_rows"),
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt64>(), "read_bytes"),
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt64>(), "peak_memory_usage"),

        /// Is there an error during the execution or commit
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt16>(), "error"),
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeString>(), "exception"),
    };
}

void PartLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(query_id);
    columns[i++]->insert(event_type);
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(duration_ms);

    columns[i++]->insert(database_name);
    columns[i++]->insert(table_name);
    columns[i++]->insert(part_name);
    columns[i++]->insert(partition_id);
    columns[i++]->insert(path_on_disk);

    columns[i++]->insert(rows);
    columns[i++]->insert(bytes_compressed_on_disk);

    Array source_part_names_array;
    source_part_names_array.reserve(source_part_names.size());
    for (const auto & name : source_part_names)
        source_part_names_array.push_back(name);

    columns[i++]->insert(source_part_names_array);

    columns[i++]->insert(bytes_uncompressed);
    columns[i++]->insert(rows_read);
    columns[i++]->insert(bytes_read_uncompressed);
    columns[i++]->insert(peak_memory_usage);

    columns[i++]->insert(error);
    columns[i++]->insert(exception);
}


bool PartLog::addNewPart(
    ContextPtr current_context, const MutableDataPartPtr & part, UInt64 elapsed_ns, const ExecutionStatus & execution_status)
{
    return addNewParts(current_context, {part}, elapsed_ns, execution_status);
}

inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}


inline UInt64 time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

bool PartLog::addNewParts(
    ContextPtr current_context, const PartLog::MutableDataPartsVector & parts, UInt64 elapsed_ns, const ExecutionStatus & execution_status)
{
    if (parts.empty())
        return true;

    std::shared_ptr<PartLog> part_log;

    try
    {
        auto table_id = parts.front()->storage.getStorageID();
        part_log = current_context->getPartLog(table_id.database_name); // assume parts belong to the same table
        if (!part_log)
            return false;

        auto query_id = CurrentThread::getQueryId();

        for (const auto & part : parts)
        {
            PartLogElement elem;

            if (query_id.data && query_id.size)
                elem.query_id.insert(0, query_id.data, query_id.size);

            elem.event_type = PartLogElement::NEW_PART;

            // construct event_time and event_time_microseconds using the same time point
            // so that the two times will always be equal up to a precision of a second.
            const auto time_now = std::chrono::system_clock::now();
            elem.event_time = time_in_seconds(time_now);
            elem.event_time_microseconds = time_in_microseconds(time_now);
            elem.duration_ms = elapsed_ns / 1000000;

            elem.database_name = table_id.database_name;
            elem.table_name = table_id.table_name;
            elem.partition_id = part->info.partition_id;
            elem.part_name = part->name;
            elem.path_on_disk = part->getFullPath();

            elem.bytes_compressed_on_disk = part->getBytesOnDisk();
            elem.rows = part->rows_count;

            elem.error = static_cast<UInt16>(execution_status.code);
            elem.exception = execution_status.message;

            part_log->add(elem);
        }
    }
    catch (...)
    {
        tryLogCurrentException(part_log ? part_log->log : &Poco::Logger::get("PartLog"), __PRETTY_FUNCTION__);
        return false;
    }

    return true;
}

}
