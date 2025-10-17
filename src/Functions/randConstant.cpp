#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

template <typename ToType, typename Name>
class ExecutableFunctionRandomConstant : public IExecutableFunction
{
public:
    explicit ExecutableFunctionRandomConstant(ToType value_) : value(value_) {}

    String getName() const override { return Name::name; }

    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeNumber<ToType>().createColumnConst(input_rows_count, value);
    }

private:
    ToType value;
};

template <typename ToType, typename Name>
class FunctionBaseRandomConstant : public IFunctionBase
{
public:
    explicit FunctionBaseRandomConstant(ToType value_, DataTypes argument_types_, DataTypePtr return_type_)
        : value(value_)
        , argument_types(std::move(argument_types_))
        , return_type(std::move(return_type_)) {}

    String getName() const override { return Name::name; }

    const DataTypes & getArgumentTypes() const override
    {
        return argument_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return return_type;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionRandomConstant<ToType, Name>>(value);
    }

    bool isDeterministic() const override
    {
        return false;
    }

private:
    ToType value;
    DataTypes argument_types;
    DataTypePtr return_type;
};

template <typename ToType, typename Name>
class RandomConstantOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = Name::name;
    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    static FunctionOverloadResolverPtr create(ContextPtr)
    {
        return std::make_unique<RandomConstantOverloadResolver<ToType, Name>>();
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & data_types) const override
    {
        size_t number_of_arguments = data_types.size();
        if (number_of_arguments > 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be 0 or 1.",
                            getName(), number_of_arguments);
        return std::make_shared<DataTypeNumber<ToType>>();
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes argument_types;

        if (!arguments.empty())
            argument_types.emplace_back(arguments.back().type);

        typename ColumnVector<ToType>::Container vec_to(1);

        TargetSpecific::Default::RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), sizeof(ToType));
        ToType value = vec_to[0];

        return std::make_unique<FunctionBaseRandomConstant<ToType, Name>>(value, argument_types, return_type);
    }
};

struct NameRandConstant { static constexpr auto name = "randConstant"; };
using FunctionBuilderRandConstant = RandomConstantOverloadResolver<UInt32, NameRandConstant>;

}

REGISTER_FUNCTION(RandConstant)
{
    FunctionDocumentation::Description description = R"(
Generates a single random value that remains constant across all rows in the current query execution.

This function:
- Returns the same random value for every row within a single query
- Produces different values across separate query executions

It is useful for applying consistent random seeds or identifiers across all rows in a dataset
    )";
    FunctionDocumentation::Syntax syntax = "randConstant([x])";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a column of type `UInt32` containing the same random value in each row.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT randConstant() AS random_value;", R"(
| random_value |
|--------------|
| 1234567890   |
        )"},
        {"Usage with parameter", "SELECT randConstant(10) AS random_value;", R"(
| random_value |
|--------------|
| 9876543210   |
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBuilderRandConstant>(documentation);
}

}


