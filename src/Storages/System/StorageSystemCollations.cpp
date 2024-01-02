#include <Columns/Collator.h>
#include <Storages/System/StorageSystemCollations.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

ColumnDescription StorageSystemCollations::getColumnDescription()
{
    return ColumnDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the collation."},
        {"language", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The language."},
    };
}

void StorageSystemCollations::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    for (const auto & [locale, lang]: AvailableCollationLocales::instance().getAvailableCollations())
    {
        res_columns[0]->insert(locale);
        res_columns[1]->insert(lang ? *lang : Field());
    }
}

}
