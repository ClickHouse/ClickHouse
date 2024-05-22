#include <Columns/Collator.h>
#include <Storages/System/StorageSystemCollations.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

ColumnsDescription StorageSystemCollations::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the collation."},
        {"language", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The language."},
    };
}

void StorageSystemCollations::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & [locale, lang]: AvailableCollationLocales::instance().getAvailableCollations())
    {
        res_columns[0]->insert(locale);
        res_columns[1]->insert(lang ? *lang : Field());
    }
}

}
