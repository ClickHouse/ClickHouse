#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/PartLog.h>


namespace DB
{

template <> struct NearestFieldTypeImpl<PartLogElement::Type> { using Type = UInt64; };

Block PartLogElement::createBlock()
{
    auto event_type_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"NEW_PART",       static_cast<Int8>(NEW_PART)},
            {"MERGE_PARTS",    static_cast<Int8>(MERGE_PARTS)},
            {"DOWNLOAD_PART",  static_cast<Int8>(DOWNLOAD_PART)},
            {"REMOVE_PART",    static_cast<Int8>(REMOVE_PART)},
            {"MUTATE_PART",    static_cast<Int8>(MUTATE_PART)},
        });

    return
    {
        {ColumnInt8::create(),   std::move(event_type_datatype),       "event_type"},
        {ColumnUInt16::create(), std::make_shared<DataTypeDate>(),     "event_date"},
        {ColumnUInt32::create(), std::make_shared<DataTypeDateTime>(), "event_time"},
        {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(),   "duration_ms"},

        {ColumnString::create(), std::make_shared<DataTypeString>(),   "database"},
        {ColumnString::create(), std::make_shared<DataTypeString>(),   "table"},
        {ColumnString::create(), std::make_shared<DataTypeString>(),   "part_name"},
        {ColumnString::create(), std::make_shared<DataTypeString>(),   "partition_id"},

        {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(),   "rows"},
        {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(),   "size_in_bytes"}, // On disk

        /// Merge-specific info
        {ColumnArray::create(ColumnString::create()), std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "merged_from"},
        {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(),   "bytes_uncompressed"}, // Result bytes
        {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(),   "read_rows"},
        {ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(),   "read_bytes"},

        /// Is there an error during the execution or commit
        {ColumnUInt16::create(), std::make_shared<DataTypeUInt16>(),   "error"},
        {ColumnString::create(), std::make_shared<DataTypeString>(),   "exception"},
    };
}

void PartLogElement::appendToBlock(Block & block) const
{
    MutableColumns columns = block.mutateColumns();

    size_t i = 0;

    columns[i++]->insert(event_type);
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time));
    columns[i++]->insert(event_time);
    columns[i++]->insert(duration_ms);

    columns[i++]->insert(database_name);
    columns[i++]->insert(table_name);
    columns[i++]->insert(part_name);
    columns[i++]->insert(partition_id);

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

    columns[i++]->insert(error);
    columns[i++]->insert(exception);

    block.setColumns(std::move(columns));
}


bool PartLog::addNewPart(Context & current_context, const MutableDataPartPtr & part, UInt64 elapsed_ns, const ExecutionStatus & execution_status)
{
    return addNewParts(current_context, {part}, elapsed_ns, execution_status);
}

bool PartLog::addNewParts(Context & current_context, const PartLog::MutableDataPartsVector & parts, UInt64 elapsed_ns,
                          const ExecutionStatus & execution_status)
{
    if (parts.empty())
        return true;

    std::shared_ptr<PartLog> part_log;

    try
    {
        part_log = current_context.getPartLog(parts.front()->storage.getDatabaseName()); // assume parts belong to the same table
        if (!part_log)
            return false;

        for (const auto & part : parts)
        {
            PartLogElement elem;

            elem.event_type = PartLogElement::NEW_PART;
            elem.event_time = time(nullptr);
            elem.duration_ms = elapsed_ns / 1000000;

            elem.database_name = part->storage.getDatabaseName();
            elem.table_name = part->storage.getTableName();
            elem.partition_id = part->info.partition_id;
            elem.part_name = part->name;

            elem.bytes_compressed_on_disk = part->bytes_on_disk;
            elem.rows = part->rows_count;

            elem.error = static_cast<UInt16>(execution_status.code);
            elem.exception = execution_status.message;

            part_log->add(elem);
        }
    }
    catch (...)
    {
        tryLogCurrentException(part_log ? part_log->log : &Logger::get("PartLog"), __PRETTY_FUNCTION__);
        return false;
    }

    return true;
}

}
