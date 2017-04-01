#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/AggregateFunctionUniqUpTo.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionUniqUpTo(const std::string & name, const DataTypes & argument_types)
{
    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniqUpTo>(*argument_types[0]));

        if (res)
            return res;
        else if (typeid_cast<const DataTypeDate     *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpTo<DataTypeDate::FieldType>>();
        else if (typeid_cast<const DataTypeDateTime*>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpTo<DataTypeDateTime::FieldType>>();
        else if (typeid_cast<const DataTypeString*>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpTo<String>>();
        else if (typeid_cast<const DataTypeTuple *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpToVariadic<true>>();
        else
            throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    else if (argument_types.size() > 1)
    {
        /// If there are several arguments, then no tuples allowed among them.
        for (const auto & type : argument_types)
            if (typeid_cast<const DataTypeTuple *>(type.get()))
                throw Exception("Tuple argument of function " + name + " must be the only argument",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<AggregateFunctionUniqUpToVariadic<false>>();
    }
    else
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

}

void registerAggregateFunctionUniqUpTo(AggregateFunctionFactory & factory)
{
    factory.registerFunction("uniqUpTo", createAggregateFunctionUniqUpTo);
}

}
