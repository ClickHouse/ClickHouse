#include <Columns/Collator.h>
#include <Storages/System/StorageSystemCollations.h>

namespace DB
{

NamesAndTypesList StorageSystemCollations::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"language", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemCollations::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    for (const auto & [locale, lang]: AvailableCollationLocales::instance().getAvailableCollations())
    {
        res_columns[0]->insert(locale);
        res_columns[1]->insert(lang);
    }
}

}
