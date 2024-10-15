#include <base/getFQDNOrHostName.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeUUID.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Common/ProfileEvents.h>
#include <DataTypes/DataTypeMap.h>

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

ColumnsDescription PartLogElement::getColumnsDescription()
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

    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    ColumnsWithTypeAndName columns_with_type_and_name;

    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"query_id", std::make_shared<DataTypeString>(), "Identifier of the INSERT query that created this data part."},
        {"event_type", std::move(event_type_datatype),
            "Type of the event that occurred with the data part. "
            "Can have one of the following values: "
            "NewPart — Inserting of a new data part, "
            "MergeParts — Merging of data parts, "
            "DownloadPart — Downloading a data part, "
            "RemovePart — Removing or detaching a data part using DETACH PARTITION, "
            "MutatePart — Mutating of a data part, "
            "MovePart — Moving the data part from the one disk to another one."},
        {"merge_reason", std::move(merge_reason_datatype),
            "The reason for the event with type MERGE_PARTS. Can have one of the following values: "
            "NotAMerge — The current event has the type other than MERGE_PARTS, "
            "RegularMerge — Some regular merge, "
            "TTLDeleteMerge — Cleaning up expired data. "
            "TTLRecompressMerge — Recompressing data part with the. "},
        {"merge_algorithm", std::move(merge_algorithm_datatype), "Merge algorithm for the event with type MERGE_PARTS. Can have one of the following values: Undecided, Horizontal, Vertical"},
        {"event_date", std::make_shared<DataTypeDate>(), "Event date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds precision."},

        {"duration_ms", std::make_shared<DataTypeUInt64>(), "Duration of this operation."},

        {"database", std::make_shared<DataTypeString>(), "Name of the database the data part is in."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table the data part is in."},
        {"table_uuid", std::make_shared<DataTypeUUID>(), "UUID of the table the data part belongs to."},
        {"part_name", std::make_shared<DataTypeString>(), "Name of the data part."},
        {"partition_id", std::make_shared<DataTypeString>(), "ID of the partition that the data part was inserted to. The column takes the `all` value if the partitioning is by `tuple()`."},
        {"partition", std::make_shared<DataTypeString>(), "The partition name."},
        {"part_type", std::make_shared<DataTypeString>(), "The type of the part. Possible values: Wide and Compact."},
        {"disk_name", std::make_shared<DataTypeString>(), "The disk name data part lies on."},
        {"path_on_disk", std::make_shared<DataTypeString>(), "Absolute path to the folder with data part files."},

        {"rows", std::make_shared<DataTypeUInt64>(), "The number of rows in the data part."},
        {"size_in_bytes", std::make_shared<DataTypeUInt64>(), "Size of the data part on disk in bytes."},

        /// Merge-specific info
        {"merged_from", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "An array of the source parts names which the current part was made up from."},
        {"bytes_uncompressed", std::make_shared<DataTypeUInt64>(), "Uncompressed size of the resulting part in bytes."},
        {"read_rows", std::make_shared<DataTypeUInt64>(), "The number of rows was read during the merge."},
        {"read_bytes", std::make_shared<DataTypeUInt64>(), "The number of bytes was read during the merge."},
        {"peak_memory_usage", std::make_shared<DataTypeUInt64>(), "The maximum amount of used during merge RAM"},

        /// Is there an error during the execution or commit
        {"error", std::make_shared<DataTypeUInt16>(), "The error code of the occurred exception."},
        {"exception", std::make_shared<DataTypeString>(), "Text message of the occurred error."},

        {"ProfileEvents", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>()), "All the profile events captured during this operation."},
    };
}

NamesAndAliases PartLogElement::getNamesAndAliases()
{
    return
    {
        {"ProfileEvents.Names", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}, "mapKeys(ProfileEvents)"},
        {"ProfileEvents.Values", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())}, "mapValues(ProfileEvents)"},
        {"name", {std::make_shared<DataTypeString>()}, "part_name"},
    };
}

void PartLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
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
    columns[i++]->insert(table_uuid);
    columns[i++]->insert(part_name);
    columns[i++]->insert(partition_id);
    columns[i++]->insert(partition);
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

    if (profile_counters)
    {
        auto * column = columns[i++].get();
        ProfileEvents::dumpToMapColumn(*profile_counters, column, true);
    }
    else
    {
        columns[i++]->insertDefault();
    }
}

bool PartLog::addNewParts(
    ContextPtr current_context, const PartLog::PartLogEntries & parts, const ExecutionStatus & execution_status)
{
    if (parts.empty())
        return true;

    std::shared_ptr<PartLog> part_log;

    try
    {
        auto table_id = parts.front().part->storage.getStorageID();
        part_log = current_context->getPartLog(table_id.database_name); // assume parts belong to the same table
        if (!part_log)
            return false;

        auto query_id = CurrentThread::getQueryId();

        for (const auto & part_log_entry : parts)
        {
            const auto & part = part_log_entry.part;

            PartLogElement elem;

            if (!query_id.empty())
                elem.query_id.insert(0, query_id.data(), query_id.size());

            elem.event_type = PartLogElement::NEW_PART;

            // construct event_time and event_time_microseconds using the same time point
            // so that the two times will always be equal up to a precision of a second.
            const auto time_now = std::chrono::system_clock::now();
            elem.event_time = timeInSeconds(time_now);
            elem.event_time_microseconds = timeInMicroseconds(time_now);
            elem.duration_ms = part_log_entry.elapsed_ns / 1000000;

            elem.database_name = table_id.database_name;
            elem.table_name = table_id.table_name;
            elem.table_uuid = table_id.uuid;
            elem.partition_id = part->info.partition_id;
            {
                WriteBufferFromString out(elem.partition);
                part->partition.serializeText(part->storage, out, {});
            }
            elem.part_name = part->name;
            elem.disk_name = part->getDataPartStorage().getDiskName();
            elem.path_on_disk = part->getDataPartStorage().getFullPath();
            elem.part_type = part->getType();

            elem.bytes_compressed_on_disk = part->getBytesOnDisk();
            elem.bytes_uncompressed = part->getBytesUncompressedOnDisk();
            elem.rows = part->rows_count;

            elem.error = static_cast<UInt16>(execution_status.code);
            elem.exception = execution_status.message;

            elem.profile_counters = part_log_entry.profile_counters;

            part_log->add(std::move(elem));
        }
    }
    catch (...)
    {
        tryLogCurrentException(part_log ? part_log->log : getLogger("PartLog"), __PRETTY_FUNCTION__);
        return false;
    }

    return true;
}

bool PartLog::addNewPart(ContextPtr context, const PartLog::PartLogEntry & part, const ExecutionStatus & execution_status)
{
    return addNewParts(context, {part}, execution_status);
}


PartLog::PartLogEntries PartLog::createPartLogEntries(const MutableDataPartsVector & parts, UInt64 elapsed_ns, ProfileCountersSnapshotPtr profile_counters)
{
    PartLogEntries part_log_entries;
    part_log_entries.reserve(parts.size());

    for (const auto & part : parts)
        part_log_entries.emplace_back(part, elapsed_ns, profile_counters);

    return part_log_entries;
}

}
