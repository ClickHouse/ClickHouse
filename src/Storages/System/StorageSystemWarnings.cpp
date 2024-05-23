#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemWarnings.h>


namespace DB
{

ColumnsDescription StorageSystemWarnings::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"message", std::make_shared<DataTypeString>(), "A warning message issued by ClickHouse server."},
    };
}

void StorageSystemWarnings::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    for (const auto & warning : context->getWarnings())
        res_columns[0]->insert(warning);
}

}
