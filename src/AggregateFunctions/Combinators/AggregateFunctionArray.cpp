#include <AggregateFunctions/Combinators/AggregateFunctionArray.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>

#include <DataTypes/DataTypeArray.h>
#include <Common/typeid_cast.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class AggregateFunctionCombinatorArray final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Array"; }

    bool supportsNesting() const override { return true; }

    bool transformsArgumentTypes() const override { return true; }

    /// The rows of the arrays are passed to the nested function one by one, so the arrays are converted around.
    VariantArgumentsOfNestedFunction getVariantArgumentsOfNestedFunction() const override { return VariantArgumentsOfNestedFunction::ConvertedAround; }

    void convertVariantArgumentsAround(
        DataTypes & arguments, const DataTypes & nested_arguments, const DataTypes & converted_nested_arguments) const override
    {
        for (size_t i = 0; i < nested_arguments.size(); ++i)
            if (!converted_nested_arguments[i]->equals(*nested_arguments[i]))
                arguments[i] = std::make_shared<DataTypeArray>(converted_nested_arguments[i]);
    }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Array aggregate functions require at least one argument");

        DataTypes nested_arguments;
        for (const auto & type : arguments)
        {
            if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(type.get()))
                nested_arguments.push_back(array->getNestedType());
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument"
                                " for aggregate function with {} suffix. Must be array.", type->getName(), getName());
        }

        return nested_arguments;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return std::make_shared<AggregateFunctionArray>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorArray(AggregateFunctionCombinatorFactory & factory);
void registerAggregateFunctionCombinatorArray(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorArray>(), Documentation{
        .description = "Applied as a suffix to an aggregate function name (e.g. `sumArray`), it makes the function take array arguments and aggregate over all of their elements.",
        .syntax = "<aggregate_function>Array",
        .related = {"ForEach", "Map"}});
}

}
