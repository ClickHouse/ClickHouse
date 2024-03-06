#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemDataTypeFamilies.h>

namespace DB
{

ColumnsDescription StorageSystemDataTypeFamilies::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Data type name."},
        {"case_insensitive", std::make_shared<DataTypeUInt8>(), "Property that shows whether you can use a data type name in a query in case insensitive manner or not. For example, `Date` and `date` are both valid."},
        {"alias_to", std::make_shared<DataTypeString>(), "Data type name for which `name` is an alias."},
    };
}

void StorageSystemDataTypeFamilies::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
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
