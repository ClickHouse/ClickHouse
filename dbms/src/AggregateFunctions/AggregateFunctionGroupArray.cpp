#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupArray.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionGroupArray(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name + ", should be 2",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupArrayNumeric>(*argument_types[0]));

    if (!res)
        res = std::make_shared<AggregateFunctionGroupArrayGeneric>();

    return res;
}


AggregateFunctionPtr createAggregateFunctionGroupArray2(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name + ", should be 1",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    bool limit_size = false;
    UInt64 max_elems = 0;

    if (parameters.empty())
    {
        // no limit
    }
    else if (parameters.size() == 1)
    {
        if (parameters[0].getType() == Field::Types::Int64 || parameters[0].getType() == Field::Types::UInt64)
        {
            if ((parameters[0].getType() == Field::Types::Int64  && parameters[0].get<Int64>() < 0) ||
                (parameters[0].getType() == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
                throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);

            limit_size = true;
            max_elems = parameters[0].get<UInt64>();
        }
        else
            throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);
    }
    else
        throw Exception("Incorrect number of parameters for aggregate function " + name + ", should be 0 or 1",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!limit_size)
    {
        if (auto res = createWithNumericType<AggregateFunctionGroupArrayNumeric2, std::false_type>(*argument_types[0]))
            return AggregateFunctionPtr(res);
        else if (typeid_cast<const DataTypeString *>(argument_types[0].get()))
            return std::make_shared<AggregateFunctionGroupArrayStringListImpl<NodeString, false>>();
        else
            return std::make_shared<AggregateFunctionGroupArrayStringListImpl<NodeGeneral, false>>();
    }
    else
    {
        if (auto res = createWithNumericType<AggregateFunctionGroupArrayNumeric2, std::true_type>(*argument_types[0], max_elems))
        {
            return AggregateFunctionPtr(res);
        }
        else if (typeid_cast<const DataTypeString *>(argument_types[0].get()))
            return std::make_shared<AggregateFunctionGroupArrayStringListImpl<NodeString, true>>(max_elems);
        else
            return std::make_shared<AggregateFunctionGroupArrayStringListImpl<NodeGeneral, true>>(max_elems);
    }
}


AggregateFunctionPtr createAggregateFunctionGroupArray4(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name + ", should be 2",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

//     if (auto res = createWithNumericType<AggregateFunctionGroupArrayNumeric2>(*argument_types[0]))
//         return AggregateFunctionPtr(res);

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
