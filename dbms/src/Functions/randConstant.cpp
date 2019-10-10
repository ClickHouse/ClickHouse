#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{

template <typename ToType, typename Name>
class PreparedFunctionRandomConstant : public PreparedFunctionImpl
{
public:
    explicit PreparedFunctionRandomConstant(ToType value_) : value(value_) {}

    String getName() const override { return Name::name; }

protected:
    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeNumber<ToType>().createColumnConst(input_rows_count, value);
    }

private:
    ToType value;
};

template <typename ToType, typename Name>
class FunctionBaseRandomConstant : public IFunctionBase
{
public:
    explicit FunctionBaseRandomConstant(ToType value_, DataTypes argument_types_)
        : value(value_)
        , argument_types(std::move(argument_types_))
        , return_type(std::make_shared<DataTypeNumber<ToType>>()) {}

    String getName() const override { return Name::name; }

    const DataTypes & getArgumentTypes() const override
    {
        return argument_types;
    }

    const DataTypePtr & getReturnType() const override
    {
        return return_type;
    }

    PreparedFunctionPtr prepare(const Block &, const ColumnNumbers &, size_t) const override
    {
        return std::make_shared<PreparedFunctionRandomConstant<ToType, Name>>(value);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

private:
    ToType value;
    DataTypes argument_types;
    DataTypePtr return_type;
};

template <typename ToType, typename Name>
class FunctionBuilderRandomConstant : public FunctionBuilderImpl
{
public:
    static constexpr auto name = Name::name;
    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    void checkNumberOfArguments(size_t number_of_arguments) const override
    {
        if (number_of_arguments > 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                            + toString(number_of_arguments) + ", should be 0 or 1.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    static FunctionBuilderPtr create(const Context &)
    {
        return std::make_shared<FunctionBuilderRandomConstant<ToType, Name>>();
    }

protected:
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeNumber<ToType>>(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        DataTypes argument_types;

        if (!arguments.empty())
            argument_types.emplace_back(arguments.back().type);

        typename ColumnVector<ToType>::Container vec_to(1);
        RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), sizeof(ToType));
        ToType value = vec_to[0];

        return std::make_shared<FunctionBaseRandomConstant<ToType, Name>>(value, argument_types);
    }
};


struct NameRandConstant { static constexpr auto name = "randConstant"; };
using FunctionBuilderRandConstant = FunctionBuilderRandomConstant<UInt32, NameRandConstant>;

void registerFunctionRandConstant(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBuilderRandConstant>();
}

}


