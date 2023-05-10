#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupArrayIntersect.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


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
class AggregateFunctionGroupArrayIntersectDate : public AggregateFunctionGroupArrayIntersect<DataTypeDate::FieldType>
{
public:
    explicit AggregateFunctionGroupArrayIntersectDate(const DataTypePtr & argument_type, const Array & parameters_)
        : AggregateFunctionGroupArrayIntersect<DataTypeDate::FieldType>(argument_type, parameters_, createResultType()) {}
    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

class AggregateFunctionGroupArrayIntersectDateTime : public AggregateFunctionGroupArrayIntersect<DataTypeDateTime::FieldType>
{
public:
    explicit AggregateFunctionGroupArrayIntersectDateTime(const DataTypePtr & argument_type, const Array & parameters_)
        : AggregateFunctionGroupArrayIntersect<DataTypeDateTime::FieldType>(argument_type, parameters_, createResultType()) {}
    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};

IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, const Array & parameters)
{   
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionGroupArrayIntersectDate(argument_type, parameters);
    else if (which.idx == TypeIndex::DateTime) return new AggregateFunctionGroupArrayIntersectDateTime(argument_type, parameters);
    else
    {
        /// Check that we can use plain version of AggregateFunctionGroupArrayIntersectGeneric
        if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            return new AggregateFunctionGroupArrayIntersectGeneric<true>(argument_type, parameters);
        else
            return new AggregateFunctionGroupArrayIntersectGeneric<false>(argument_type, parameters);
    }
}

inline AggregateFunctionPtr createAggregateFunctionGroupArrayIntersectImpl(const std::string & name, const DataTypePtr & argument_type, const Array & parameters)
{
    const auto & nested_type = dynamic_cast<const DataTypeArray &>(*argument_type).getNestedType();
    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupArrayIntersect, const DataTypePtr &>(*nested_type, nested_type, parameters));
    if (!res)
    {
        res = AggregateFunctionPtr(createWithExtraTypes(nested_type, parameters));
    }

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                        argument_type->getName(), name);

    return res;
}

AggregateFunctionPtr createAggregateFunctionGroupArrayIntersect(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    if (!WhichDataType(argument_types.at(0)).isArray()) {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function groupArrayIntersect accepts only array type argument.");
    }

    if (!parameters.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Incorrect number of parameters for aggregate function {}, should be 0", name);

    return createAggregateFunctionGroupArrayIntersectImpl(name, argument_types[0], parameters);
}

}

void registerAggregateFunctionGroupArrayIntersect(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("groupArrayIntersect", { createAggregateFunctionGroupArrayIntersect, properties });
}

}
