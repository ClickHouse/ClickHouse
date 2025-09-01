#include <Interpreters/IcebergMetadataLog.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/Context.h>
#include <Processors/LimitTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Access/ContextAccess.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Core/Settings.h>
#include <base/Decimal.h>

namespace DB
{

namespace Setting
{
extern const SettingsUInt64 iceberg_metadata_log_level;
}

namespace ErrorCodes
{
extern const int CANNOT_CLOCK_GETTIME;
}


ColumnsDescription IcebergMetadataLogElement::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the entry."},
        {"event_time",std::make_shared<DataTypeDateTime>(),"Event time."},
        {"query_id",std::make_shared<DataTypeString>(),"Query id."},
        {"content_type",std::make_shared<DataTypeInt64>(),"Content type."},
        {"path",std::make_shared<DataTypeString>(),"Path of table."},
        {"file_name",std::make_shared<DataTypeString>(),"Name of file."},
        {"content",std::make_shared<DataTypeString>(),"Content of metadata."}
    };
}

void IcebergMetadataLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_index = 0;
    auto event_time_seconds = current_time / 1000000;
    columns[column_index++]->insert(DateLUT::instance().toDayNum(event_time_seconds).toUnderType());
    columns[column_index++]->insert(current_time);
    columns[column_index++]->insert(query_id);
    columns[column_index++]->insert(content_type);
    columns[column_index++]->insert(path);
    columns[column_index++]->insert(filename);
    columns[column_index++]->insert(metadata_content);
}

void insertRowToLogTable(
    const ContextPtr & local_context, String row, IcebergMetadataLogLevel row_log_level, const String & file_path, const String & filename)
{
    UInt64 set_log_level = local_context->getSettingsRef()[Setting::iceberg_metadata_log_level].value;
    if (set_log_level < static_cast<UInt64>(row_log_level))
        return;
    timespec spec{};
    if (clock_gettime(CLOCK_REALTIME, &spec))
        throw ErrnoException(ErrorCodes::CANNOT_CLOCK_GETTIME, "Cannot clock_gettime");

    Context::getGlobalContextInstance()->getIcebergMetadataLog()->add(
        DB::IcebergMetadataLogElement{
            .current_time = spec.tv_sec,
            .query_id = local_context->getCurrentQueryId(),
            .content_type = row_log_level,
            .path = file_path,
            .filename = filename,
            .metadata_content = row});
}

IcebergMetadataLogLevel getIcebergMetadataLogLevelFromSettings(const ContextPtr & local_context)
{
    return static_cast<IcebergMetadataLogLevel>(local_context->getSettingsRef()[Setting::iceberg_metadata_log_level].value);
}
}
