#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Storages/System/StorageSystemAggregateFunctionCombinators.h>

namespace DB
{

ColumnsDescription StorageSystemAggregateFunctionCombinators::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the combinator."},
        {"is_internal", std::make_shared<DataTypeUInt8>(), "Whether this combinator is for internal usage only."},
    };
}

void StorageSystemAggregateFunctionCombinators::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & combinators = AggregateFunctionCombinatorFactory::instance().getAllAggregateFunctionCombinators();
    for (const auto & pair : combinators)
    {
        res_columns[0]->insert(pair.name);
        res_columns[1]->insert(pair.combinator_ptr->isForInternalUsageOnly());
    }
}

}
