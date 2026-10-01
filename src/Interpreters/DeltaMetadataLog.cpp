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
#include <Interpreters/DeltaMetadataLog.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/LimitTransform.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <base/Decimal.h>
#include <Common/DateLUTImpl.h>
#include <Common/typeid_cast.h>
#include <Common/ErrnoException.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CLOCK_GETTIME;
extern const int BAD_ARGUMENTS;
}

namespace Setting
{
extern const SettingsBool delta_lake_log_metadata;
}

namespace
{

const DataTypePtr rowType = makeNullable(std::make_shared<DataTypeUInt64>());

}

ColumnsDescription DeltaMetadataLogElement::getColumnsDescription()
{
    return ColumnsDescription{
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the entry."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},
        {"query_id", std::make_shared<DataTypeString>(), "Query id."},
        {"table_path", std::make_shared<DataTypeString>(), "Table path."},
        {"file_path", std::make_shared<DataTypeString>(), "File path."},
        {"content", std::make_shared<DataTypeString>(), "Content in a JSON format."}};
}

void DeltaMetadataLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_index = 0;
    columns[column_index++]->insert(DateLUT::instance().toDayNum(current_time).toUnderType());
    columns[column_index++]->insert(current_time);
    columns[column_index++]->insert(query_id);
    columns[column_index++]->insert(table_path);
    columns[column_index++]->insert(file_path);
    columns[column_index++]->insert(metadata_content);
}

void insertDeltaRowToLogTable(
    const ContextPtr & local_context,
    String row,
    const String & table_path,
    const String & file_path)
{
    if (!local_context->getSettingsRef()[Setting::delta_lake_log_metadata].value)
        return;

    timespec spec{};
    if (clock_gettime(CLOCK_REALTIME, &spec))
        throw ErrnoException(ErrorCodes::CANNOT_CLOCK_GETTIME, "Cannot clock_gettime");

    auto delta_lake_metadata_log = Context::getGlobalContextInstance()->getDeltaMetadataLog();
    if (!delta_lake_metadata_log)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Delta metadata log table is not configured");
    delta_lake_metadata_log->add(
        DB::DeltaMetadataLogElement{
            .current_time = spec.tv_sec,
            .query_id = local_context->getCurrentQueryId(),
            .table_path = table_path,
            .file_path = file_path,
            .metadata_content = row});
}
}
