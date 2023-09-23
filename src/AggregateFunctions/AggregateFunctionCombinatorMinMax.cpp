#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/AggregateFunctionCombinatorMinMax.h>
#include <AggregateFunctions/AggregateFunctionMinMaxAny.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
template <template <typename> class Data>
class AggregateFunctionCombinatorMinMax final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return Data<SingleValueDataGeneric<>>::name(); }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with {} suffix",
                getName());

        return DataTypes(arguments.begin(), arguments.end() - 1);
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        const DataTypePtr & argument_type = arguments.back();
        WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return std::make_shared<AggregateFunctionMinMax<Data<SingleValueDataFixed<TYPE>>>>(nested_function, arguments, params); /// NOLINT
        FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

        if (which.idx == TypeIndex::Date)
            return std::make_shared<AggregateFunctionMinMax<Data<SingleValueDataFixed<DataTypeDate::FieldType>>>>(
                nested_function, arguments, params);
        if (which.idx == TypeIndex::DateTime)
            return std::make_shared<AggregateFunctionMinMax<Data<SingleValueDataFixed<DataTypeDateTime::FieldType>>>>(
                nested_function, arguments, params);
        if (which.idx == TypeIndex::DateTime64)
            return std::make_shared<AggregateFunctionMinMax<Data<SingleValueDataFixed<DateTime64>>>>(nested_function, arguments, params);
        if (which.idx == TypeIndex::Decimal32)
            return std::make_shared<AggregateFunctionMinMax<Data<SingleValueDataFixed<Decimal32>>>>(nested_function, arguments, params);
        if (which.idx == TypeIndex::Decimal64)
            return std::make_shared<AggregateFunctionMinMax<Data<SingleValueDataFixed<Decimal64>>>>(nested_function, arguments, params);
        if (which.idx == TypeIndex::Decimal128)
            return std::make_shared<AggregateFunctionMinMax<Data<SingleValueDataFixed<Decimal128>>>>(nested_function, arguments, params);
        if (which.idx == TypeIndex::Decimal256)
            return std::make_shared<AggregateFunctionMinMax<Data<SingleValueDataFixed<Decimal256>>>>(nested_function, arguments, params);
        if (which.idx == TypeIndex::String)
            return std::make_shared<AggregateFunctionMinMax<Data<SingleValueDataString>>>(nested_function, arguments, params);

        return std::make_shared<AggregateFunctionMinMax<Data<SingleValueDataGeneric<>>>>(nested_function, arguments, params);
    }
};

template <typename Data>
struct AggregateFunctionMinDataCapitalized : AggregateFunctionMinData<Data>
{
    static const char * name() { return "Min"; }
};

template <typename Data>
struct AggregateFunctionMaxDataCapitalized : AggregateFunctionMaxData<Data>
{
    static const char * name() { return "Max"; }
};

}

void registerAggregateFunctionCombinatorMinMax(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorMinMax<AggregateFunctionMinDataCapitalized>>());
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorMinMax<AggregateFunctionMaxDataCapitalized>>());
}

}
