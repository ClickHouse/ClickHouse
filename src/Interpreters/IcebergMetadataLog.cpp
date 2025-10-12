#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/IcebergMetadataLog.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/LimitTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <base/Decimal.h>
#include <Common/DateLUTImpl.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace Setting
{
extern const SettingsIcebergMetadataLogLevel iceberg_metadata_log_level;
}

namespace ErrorCodes
{
extern const int CANNOT_CLOCK_GETTIME;
}

namespace
{

const DataTypePtr rowType = makeNullable(std::make_shared<DataTypeUInt64>());

}

ColumnsDescription IcebergMetadataLogElement::getColumnsDescription()
{
    auto iceberg_metadata_log_entry_datatype = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"None", static_cast<Int8>(IcebergMetadataLogLevel::None)},
        {"Metadata", static_cast<Int8>(IcebergMetadataLogLevel::Metadata)},
        {"ManifestListMetadata", static_cast<Int8>(IcebergMetadataLogLevel::ManifestListMetadata)},
        {"ManifestListEntry", static_cast<Int8>(IcebergMetadataLogLevel::ManifestListEntry)},
        {"ManifestFileMetadata", static_cast<Int8>(IcebergMetadataLogLevel::ManifestFileMetadata)},
        {"ManifestFileEntry", static_cast<Int8>(IcebergMetadataLogLevel::ManifestFileEntry)}});

    return ColumnsDescription{
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the entry."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},
        {"query_id", std::make_shared<DataTypeString>(), "Query id."},
        {"content_type", std::move(iceberg_metadata_log_entry_datatype), "Content type."},
        {"table_path", std::make_shared<DataTypeString>(), "Table path."},
        {"file_path", std::make_shared<DataTypeString>(), "File path."},
        {"content", std::make_shared<DataTypeString>(), "Content in a JSON format (json file content, avro metadata or avro entry)."},
        {"row_in_file", rowType, "Row in file."}};
}

void IcebergMetadataLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_index = 0;
    auto event_time_seconds = current_time / 1000000;
    columns[column_index++]->insert(DateLUT::instance().toDayNum(event_time_seconds).toUnderType());
    columns[column_index++]->insert(current_time);
    columns[column_index++]->insert(query_id);
    columns[column_index++]->insert(content_type);
    columns[column_index++]->insert(table_path);
    columns[column_index++]->insert(file_path);
    columns[column_index++]->insert(metadata_content);
    columns[column_index++]->insert(row_in_file ? *row_in_file : rowType->getDefault());
}

void insertRowToLogTable(
    const ContextPtr & local_context,
    String row,
    IcebergMetadataLogLevel row_log_level,
    const String & table_path,
    const String & file_path,
    std::optional<UInt64> row_in_file)
{
    IcebergMetadataLogLevel set_log_level = local_context->getSettingsRef()[Setting::iceberg_metadata_log_level].value;
    if (set_log_level < row_log_level)
        return;
    timespec spec{};
    if (clock_gettime(CLOCK_REALTIME, &spec))
        throw ErrnoException(ErrorCodes::CANNOT_CLOCK_GETTIME, "Cannot clock_gettime");

    Context::getGlobalContextInstance()->getIcebergMetadataLog()->add(
        DB::IcebergMetadataLogElement{
            .current_time = spec.tv_sec,
            .query_id = local_context->getCurrentQueryId(),
            .content_type = row_log_level,
            .table_path = table_path,
            .file_path = file_path,
            .metadata_content = row,
            .row_in_file = row_in_file});
}
}
