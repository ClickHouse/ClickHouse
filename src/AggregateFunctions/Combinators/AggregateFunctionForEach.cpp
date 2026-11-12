#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionForEach.h>

#include <DataTypes/DataTypeArray.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class AggregateFunctionCombinatorForEach final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "ForEach"; }

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
        DataTypes nested_arguments;
        for (const auto & type : arguments)
        {
            if (const DataTypeArray * array = typeid_cast<const DataTypeArray *>(type.get()))
                nested_arguments.push_back(array->getNestedType());
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for "
                                "aggregate function with {} suffix. Must be array.", type->getName(), getName());
        }

        return nested_arguments;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return std::make_shared<AggregateFunctionForEach>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorForEach(AggregateFunctionCombinatorFactory & factory);
void registerAggregateFunctionCombinatorForEach(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorForEach>(), Documentation{
        .description = "Applied as a suffix to an aggregate function name (e.g. `sumForEach`), it aggregates arrays element-wise, producing an array of per-position results.",
        .syntax = "<aggregate_function>ForEach",
        .related = {"Array"}});
}

}
