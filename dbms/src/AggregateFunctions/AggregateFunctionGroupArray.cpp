#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupArray.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionGroupArray(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name + ", should be 2",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupArrayNumeric>(*argument_types[0]));

    if (!res)
        res = std::make_shared<AggregateFunctionGroupArrayGeneric>();

    return res;
}

AggregateFunctionPtr createAggregateFunctionGroupArray2(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name + ", should be 1",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (auto res = createWithNumericType<AggregateFunctionGroupArrayNumeric2>(*argument_types[0]))
        return AggregateFunctionPtr(res);

    if (typeid_cast<const DataTypeString *>(argument_types[0].get()))
        return std::make_shared<AggregateFunctionGroupArrayStringListImpl<NodeString>>();
    else
        return std::make_shared<AggregateFunctionGroupArrayStringListImpl<NodeGeneral>>();
}


AggregateFunctionPtr createAggregateFunctionGroupArray4(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name + ", should be 2",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (auto res = createWithNumericType<AggregateFunctionGroupArrayNumeric2>(*argument_types[0]))
        return AggregateFunctionPtr(res);

    if (typeid_cast<const DataTypeString *>(argument_types[0].get()))
        return std::make_shared<AggregateFunctionGroupArrayStringConcatImpl>();
    else
        return std::make_shared<AggregateFunctionGroupArrayGeneric_ColumnPtrImpl>();
}

}

void registerAggregateFunctionGroupArray(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupArray", createAggregateFunctionGroupArray);
    factory.registerFunction("groupArray2", createAggregateFunctionGroupArray2);
    factory.registerFunction("groupArray4", createAggregateFunctionGroupArray4);
}

}
