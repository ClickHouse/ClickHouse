#include <Columns/Collator.h>
#include <Storages/System/StorageSystemCollations.h>

namespace DB
{

NamesAndTypesList StorageSystemCollations::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemCollations::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    for (const auto & collation_name : Collator::getAvailableCollations())
        res_columns[0]->insert(collation_name);
}

}
