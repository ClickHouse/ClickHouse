#include <Processors/QueryPlan/Optimizations/keyTypeBreaksHashSharding.h>

#include <DataTypes/IDataType.h>

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

    if (breaks_sharding(type))
        return true;

    bool result = false;
    type.forEachChild([&](const IDataType & child)
    {
        if (breaks_sharding(child))
            result = true;
    });
    return result;
}

}

}
