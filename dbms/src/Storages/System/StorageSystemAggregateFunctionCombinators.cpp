#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <Storages/System/StorageSystemAggregateFunctionCombinators.h>

namespace DB
{
void StorageSystemAggregateFunctionCombinators::fillData(MutableColumns & res_columns) const
{
    const auto & combinators = AggregateFunctionCombinatorFactory::instance().getAllAggregateFunctionCombinators();
    for (const auto & pair : combinators)
    {
        res_columns[0]->insert(pair.first);
        if (pair.first != "Null")
            res_columns[1]->insert(UInt64(0));
        else
            res_columns[1]->insert(UInt64(1));
    }
}
}
