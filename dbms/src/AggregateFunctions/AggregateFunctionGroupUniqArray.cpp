#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupUniqArray.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Substitute return type for Date and DateTime
template <typename has_limit>
class AggregateFunctionGroupUniqArrayDate : public AggregateFunctionGroupUniqArray<DataTypeDate::FieldType, has_limit>
{
public:
    AggregateFunctionGroupUniqArrayDate(const DataTypePtr & argument_type, UInt64 max_elems_ = std::numeric_limits<UInt64>::max()) : AggregateFunctionGroupUniqArray<DataTypeDate::FieldType, has_limit>(argument_type, max_elems_) {}
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

template <typename has_limit>
class AggregateFunctionGroupUniqArrayDateTime : public AggregateFunctionGroupUniqArray<DataTypeDateTime::FieldType, has_limit>
{
public:
    AggregateFunctionGroupUniqArrayDateTime(const DataTypePtr & argument_type, UInt64 max_elems_ = std::numeric_limits<UInt64>::max()) : AggregateFunctionGroupUniqArray<DataTypeDateTime::FieldType, has_limit>(argument_type, max_elems_) {}
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};

template <typename has_limit, typename ... TArgs>
static IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionGroupUniqArrayDate<has_limit>(argument_type, std::forward<TArgs>(args)...);
    else if (which.idx == TypeIndex::DateTime) return new AggregateFunctionGroupUniqArrayDateTime<has_limit>(argument_type, std::forward<TArgs>(args)...);
    else
    {
        /// Check that we can use plain version of AggreagteFunctionGroupUniqArrayGeneric
        if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            return new AggreagteFunctionGroupUniqArrayGeneric<true, has_limit>(argument_type, std::forward<TArgs>(args)...);
        else
            return new AggreagteFunctionGroupUniqArrayGeneric<false, has_limit>(argument_type, std::forward<TArgs>(args)...);
    }
}

template <typename has_limit, typename ... TArgs>
inline AggregateFunctionPtr createAggregateFunctionGroupUniqArrayImpl(const std::string & name, const DataTypePtr & argument_type, TArgs ... args)
{

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupUniqArray, has_limit, const DataTypePtr &, TArgs...>(*argument_type, argument_type, std::forward<TArgs>(args)...));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes<has_limit>(argument_type, std::forward<TArgs>(args)...));

    if (!res)
        throw Exception("Illegal type " + argument_type->getName() +
                        " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;

}

AggregateFunctionPtr createAggregateFunctionGroupUniqArray(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertUnary(name, argument_types);

    bool limit_size = false;
    UInt64 max_elems = std::numeric_limits<UInt64>::max();

    if (parameters.empty())
    {
        // no limit
    }
    else if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
            throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);

        if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0) ||
            (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
            throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);

        limit_size = true;
        max_elems = parameters[0].get<UInt64>();
    }
    else
        throw Exception("Incorrect number of parameters for aggregate function " + name + ", should be 0 or 1",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!limit_size)
        return createAggregateFunctionGroupUniqArrayImpl<std::false_type>(name, argument_types[0]);
    else
        return createAggregateFunctionGroupUniqArrayImpl<std::true_type>(name, argument_types[0], max_elems);
}

}

void registerAggregateFunctionGroupUniqArray(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupUniqArray", createAggregateFunctionGroupUniqArray);
}

}
