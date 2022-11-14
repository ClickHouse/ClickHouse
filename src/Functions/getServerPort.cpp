#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class ExecutableFunctionGetServerPort : public IExecutableFunction
{
public:
    explicit ExecutableFunctionGetServerPort(UInt16 port_) : port(port_) {}

    String getName() const override { return "getServerPort"; }

    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeNumber<UInt16>().createColumnConst(input_rows_count, port);
    }

private:
    UInt16 port;
};

class FunctionBaseGetServerPort : public IFunctionBase
{
public:
    explicit FunctionBaseGetServerPort(bool is_distributed_, UInt16 port_, DataTypes argument_types_, DataTypePtr return_type_)
        : is_distributed(is_distributed_), port(port_), argument_types(std::move(argument_types_)), return_type(std::move(return_type_))
    {
    }

    String getName() const override { return "getServerPort"; }

    const DataTypes & getArgumentTypes() const override
    {
        return argument_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return return_type;
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isSuitableForConstantFolding() const override { return !is_distributed; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionGetServerPort>(port);
    }

private:
    bool is_distributed;
    UInt16 port;
    DataTypes argument_types;
    DataTypePtr return_type;
};

class GetServerPortOverloadResolver : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto name = "getServerPort";

    String getName() const override { return name; }

    static FunctionOverloadResolverPtr create(ContextPtr context_)
    {
        return std::make_unique<GetServerPortOverloadResolver>(context_);
    }

    explicit GetServerPortOverloadResolver(ContextPtr context_) : WithContext(context_) {}

    size_t getNumberOfArguments() const override { return 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & data_types) const override
    {
        size_t number_of_arguments = data_types.size();
        if (number_of_arguments != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1",
                getName(),
                number_of_arguments);
        return std::make_shared<DataTypeNumber<UInt16>>();
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (!isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The argument of function {} should be a constant string with the name of a setting",
                getName());
        const auto * column = arguments[0].column.get();
        if (!column || !checkAndGetColumnConstStringOrFixedString(column))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The argument of function {} should be a constant string with the name of a setting",
                getName());

        String port_name{column->getDataAt(0)};
        auto port = getContext()->getServerPort(port_name);

        DataTypes argument_types;
        argument_types.emplace_back(arguments.back().type);
        return std::make_unique<FunctionBaseGetServerPort>(getContext()->isDistributed(), port, argument_types, return_type);
    }
};

}

void registerFunctionGetServerPort(FunctionFactory & factory)
{
    factory.registerFunction<GetServerPortOverloadResolver>();
}

}
