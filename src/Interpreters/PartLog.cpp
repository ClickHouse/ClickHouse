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

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

PartLogElement::MergeReasonType PartLogElement::getMergeReasonType(MergeType merge_type)
{
    switch (merge_type)
    {
        case MergeType::Regular:
            return REGULAR_MERGE;
        case MergeType::TTLDelete:
            return TTL_DELETE_MERGE;
        case MergeType::TTLRecompress:
            return TTL_RECOMPRESS_MERGE;
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown MergeType {}", static_cast<UInt64>(merge_type));
}

PartLogElement::PartMergeAlgorithm PartLogElement::getMergeAlgorithm(MergeAlgorithm merge_algorithm_)
{
    switch (merge_algorithm_)
    {
        case MergeAlgorithm::Undecided:
            return UNDECIDED;
        case MergeAlgorithm::Horizontal:
            return HORIZONTAL;
        case MergeAlgorithm::Vertical:
            return VERTICAL;
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown MergeAlgorithm {}", static_cast<UInt64>(merge_algorithm_));
}

NamesAndTypesList PartLogElement::getNamesAndTypes()
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

    auto merge_reason_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"NotAMerge",           static_cast<Int8>(NOT_A_MERGE)},
            {"RegularMerge",        static_cast<Int8>(REGULAR_MERGE)},
            {"TTLDeleteMerge",      static_cast<Int8>(TTL_DELETE_MERGE)},
            {"TTLRecompressMerge",  static_cast<Int8>(TTL_RECOMPRESS_MERGE)},
        }
    );

    auto merge_algorithm_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Undecided",  static_cast<Int8>(UNDECIDED)},
            {"Horizontal", static_cast<Int8>(HORIZONTAL)},
            {"Vertical",   static_cast<Int8>(VERTICAL)},
        }
    );

    ColumnsWithTypeAndName columns_with_type_and_name;

    return {
        {"query_id", std::make_shared<DataTypeString>()},
        {"event_type", std::move(event_type_datatype)},
        {"merge_reason", std::move(merge_reason_datatype)},
        {"merge_algorithm", std::move(merge_algorithm_datatype)},
        {"event_date", std::make_shared<DataTypeDate>()},

        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},

        {"duration_ms", std::make_shared<DataTypeUInt64>()},

        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"part_name", std::make_shared<DataTypeString>()},
        {"partition_id", std::make_shared<DataTypeString>()},
        {"part_type", std::make_shared<DataTypeString>()},
        {"disk_name", std::make_shared<DataTypeString>()},
        {"path_on_disk", std::make_shared<DataTypeString>()},

        {"rows", std::make_shared<DataTypeUInt64>()},
        {"size_in_bytes", std::make_shared<DataTypeUInt64>()}, // On disk

        /// Merge-specific info
        {"merged_from", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"bytes_uncompressed", std::make_shared<DataTypeUInt64>()}, // Result bytes
        {"read_rows", std::make_shared<DataTypeUInt64>()},
        {"read_bytes", std::make_shared<DataTypeUInt64>()},
        {"peak_memory_usage", std::make_shared<DataTypeUInt64>()},

        /// Is there an error during the execution or commit
        {"error", std::make_shared<DataTypeUInt16>()},
        {"exception", std::make_shared<DataTypeString>()},
    };
}

void PartLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(query_id);
    columns[i++]->insert(event_type);
    columns[i++]->insert(merge_reason);
    columns[i++]->insert(merge_algorithm);
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(duration_ms);

    columns[i++]->insert(database_name);
    columns[i++]->insert(table_name);
    columns[i++]->insert(part_name);
    columns[i++]->insert(partition_id);
    columns[i++]->insert(part_type.toString());
    columns[i++]->insert(disk_name);
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

static inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}


static inline UInt64 time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
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

            if (!query_id.empty())
                elem.query_id.insert(0, query_id.data(), query_id.size());

            elem.event_type = PartLogElement::NEW_PART; //-V1048

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
            elem.disk_name = part->data_part_storage->getDiskName();
            elem.path_on_disk = part->data_part_storage->getFullPath();
            elem.part_type = part->getType();

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
