#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemDataTypeFamilies.h>

namespace DB
{

NamesAndTypesList StorageSystemDataTypeFamilies::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"case_insensitive", std::make_shared<DataTypeUInt8>()},
        {"alias_to", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemDataTypeFamilies::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    const auto & factory = DataTypeFactory::instance();
    auto names = factory.getAllRegisteredNames();
    for (const auto & dtf_name : names)
    {
        res_columns[0]->insert(dtf_name);
        res_columns[1]->insert(factory.isCaseInsensitive(dtf_name));

        if (factory.isAlias(dtf_name))
            res_columns[2]->insert(factory.aliasTo(dtf_name));
        else
            res_columns[2]->insertDefault();
    }
}

}
