#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/System/StorageSystemWarnings.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

ColumnsDescription StorageSystemWarnings::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"message", std::make_shared<DataTypeString>(), "A warning message issued by ClickHouse server."},
        {"message_format_string", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "A format string that was used to format the message."},
    };
}

void StorageSystemWarnings::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & [_, warning] : context->getWarnings())
    {
        res_columns[0]->insert(warning.text);
        res_columns[1]->insert(warning.format_string);
    }
}

}
