#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/AggregateFunctionUniqUpTo.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


namespace
{

static constexpr UInt8 uniq_upto_max_threshold = 100;


AggregateFunctionPtr createAggregateFunctionUniqUpTo(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    UInt8 threshold = 5;    /// default value

    if (!params.empty())
    {
        if (params.size() != 1)
            throw Exception("Aggregate function " + name + " requires one parameter or less.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 threshold_param = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (threshold_param > uniq_upto_max_threshold)
            throw Exception("Too large parameter for aggregate function " + name + ". Maximum: " + toString(uniq_upto_max_threshold),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        threshold = threshold_param;
    }

    if (argument_types.size() == 1)
    {
        const IDataType & argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniqUpTo>(*argument_types[0], threshold));

        if (res)
            return res;
        else if (typeid_cast<const DataTypeDate *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpTo<DataTypeDate::FieldType>>(threshold);
        else if (typeid_cast<const DataTypeDateTime *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpTo<DataTypeDateTime::FieldType>>(threshold);
        else if (typeid_cast<const DataTypeString *>(&argument_type) || typeid_cast<const DataTypeFixedString*>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpTo<String>>(threshold);
        else if (typeid_cast<const DataTypeTuple *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpToVariadic<true>>(argument_types, threshold);
        else if (typeid_cast<const DataTypeUUID *>(&argument_type))
            return std::make_shared<AggregateFunctionUniqUpTo<DataTypeUUID::FieldType>>(threshold);
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

        return std::make_shared<AggregateFunctionUniqUpToVariadic<false>>(argument_types, threshold);
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
