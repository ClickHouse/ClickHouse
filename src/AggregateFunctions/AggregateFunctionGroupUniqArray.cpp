#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupUniqArray.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Substitute return type for Date and DateTime
template <typename HasLimit>
class AggregateFunctionGroupUniqArrayDate : public AggregateFunctionGroupUniqArray<DataTypeDate::FieldType, HasLimit>
{
public:
    explicit AggregateFunctionGroupUniqArrayDate(const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : AggregateFunctionGroupUniqArray<DataTypeDate::FieldType, HasLimit>(argument_type, parameters_, createResultType(), max_elems_) {}
    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

template <typename HasLimit>
class AggregateFunctionGroupUniqArrayDateTime : public AggregateFunctionGroupUniqArray<DataTypeDateTime::FieldType, HasLimit>
{
public:
    explicit AggregateFunctionGroupUniqArrayDateTime(const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : AggregateFunctionGroupUniqArray<DataTypeDateTime::FieldType, HasLimit>(argument_type, parameters_, createResultType(), max_elems_) {}
    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};

template <typename HasLimit>
class AggregateFunctionGroupUniqArrayIPv4 : public AggregateFunctionGroupUniqArray<DataTypeIPv4::FieldType, HasLimit>
{
public:
    explicit AggregateFunctionGroupUniqArrayIPv4(const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : AggregateFunctionGroupUniqArray<DataTypeIPv4::FieldType, HasLimit>(argument_type, parameters_, createResultType(), max_elems_) {}
    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeIPv4>()); }
};

template <typename HasLimit, typename ... TArgs>
IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionGroupUniqArrayDate<HasLimit>(argument_type, std::forward<TArgs>(args)...);
    else if (which.idx == TypeIndex::DateTime) return new AggregateFunctionGroupUniqArrayDateTime<HasLimit>(argument_type, std::forward<TArgs>(args)...);
    else if (which.idx == TypeIndex::IPv4) return new AggregateFunctionGroupUniqArrayIPv4<HasLimit>(argument_type, std::forward<TArgs>(args)...);
    else
    {
        /// Check that we can use plain version of AggregateFunctionGroupUniqArrayGeneric
        if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            return new AggregateFunctionGroupUniqArrayGeneric<true, HasLimit>(argument_type, std::forward<TArgs>(args)...);
        else
            return new AggregateFunctionGroupUniqArrayGeneric<false, HasLimit>(argument_type, std::forward<TArgs>(args)...);
    }
}

template <typename HasLimit, typename ... TArgs>
inline AggregateFunctionPtr createAggregateFunctionGroupUniqArrayImpl(const std::string & name, const DataTypePtr & argument_type, TArgs ... args)
{

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupUniqArray, HasLimit, const DataTypePtr &, TArgs...>(*argument_type, argument_type, std::forward<TArgs>(args)...));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes<HasLimit>(argument_type, std::forward<TArgs>(args)...));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                        argument_type->getName(), name);

    return res;

}

AggregateFunctionPtr createAggregateFunctionGroupUniqArray(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0) ||
            (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        limit_size = true;
        max_elems = parameters[0].get<UInt64>();
    }
    else
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Incorrect number of parameters for aggregate function {}, should be 0 or 1", name);

    if (!limit_size)
        return createAggregateFunctionGroupUniqArrayImpl<std::false_type>(name, argument_types[0], parameters);
    else
        return createAggregateFunctionGroupUniqArrayImpl<std::true_type>(name, argument_types[0], parameters, max_elems);
}

}

void registerAggregateFunctionGroupUniqArray(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("groupUniqArray", { createAggregateFunctionGroupUniqArray, properties });
}

}
