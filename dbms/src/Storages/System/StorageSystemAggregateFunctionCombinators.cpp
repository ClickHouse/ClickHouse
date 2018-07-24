#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <Storages/System/StorageSystemAggregateFunctionCombinators.h>

namespace DB
{

NamesAndTypesList StorageSystemAggregateFunctionCombinators::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"is_internal", std::make_shared<DataTypeUInt8>()},
    };
}

void StorageSystemAggregateFunctionCombinators::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    const auto & combinators = AggregateFunctionCombinatorFactory::instance().getAllAggregateFunctionCombinators();
    for (const auto & pair : combinators)
    {
        res_columns[0]->insert(pair.first);
        if (pair.second->isForInternalUsageOnly())
            res_columns[1]->insert(UInt64(1));
        else
            res_columns[1]->insert(UInt64(0));
    }
}

}
