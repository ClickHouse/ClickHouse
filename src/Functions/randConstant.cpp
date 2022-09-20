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

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

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
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                            + toString(number_of_arguments) + ", should be 0 or 1.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
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
    factory.registerFunction<FunctionBuilderRandConstant>();
}

}


