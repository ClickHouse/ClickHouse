#include <Processors/QueryPlan/Optimizations/keyTypeBreaksHashSharding.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/TypeTree.h>

namespace DB
{
namespace QueryPlanOptimizations
{

bool keyTypeBreaksHashSharding(const IDataType & type)
{
    auto breaks_sharding = [](const IDataType & t)
    {
        WhichDataType which(t);
        return which.isFloat() || which.isObject() || which.isDynamic();
    };

    return anyInTypeTree(type, breaks_sharding);
}

}

}
