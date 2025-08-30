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


}
