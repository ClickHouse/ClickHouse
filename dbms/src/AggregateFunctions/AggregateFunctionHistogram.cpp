#include "AggregateFunctionHistogram.h"
#include "AggregateFunctionFactory.h"
#include "FactoryHelpers.h"
#include "Helpers.h"

#include <Common/FieldVisitors.h>

namespace DB {

AggregateFunctionPtr createAggregateFunctionHistogram(const std::string & name, const DataTypes & arguments, const Array & params)
{
    if (params.size() != 1)
    {
        throw Exception("Function " + name + " requires only bins count");
    }
    assertUnary(name, arguments);

    UInt32 bins_count;

#define READ(VAL, PARAM) \
    VAL = applyVisitor(FieldVisitorConvertToNumber<decltype(VAL)>(), PARAM);

    READ(bins_count, params[0]);
#undef READ

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionHistogram>(*arguments[0], bins_count));

    return res;
}

void registerAggregateFunctionHistogram(AggregateFunctionFactory & factory)
{
    factory.registerFunction("histogram", createAggregateFunctionHistogram);
}

}
