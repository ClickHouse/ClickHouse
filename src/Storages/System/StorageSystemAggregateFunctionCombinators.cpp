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

void StorageSystemAggregateFunctionCombinators::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    const auto & combinators = AggregateFunctionCombinatorFactory::instance().getAllAggregateFunctionCombinators();
    for (const auto & pair : combinators)
    {
        res_columns[0]->insert(pair.name);
        res_columns[1]->insert(pair.combinator_ptr->isForInternalUsageOnly());
    }
}

}
