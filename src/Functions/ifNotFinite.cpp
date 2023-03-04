#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Core/ColumnNumbers.h>


namespace DB
{
namespace
{

/// ifNotFinite(x, y) is equivalent to isFinite(x) ? x : y.
class FunctionIfNotFinite : public IFunction, WithContext
{
public:
    static constexpr auto name = "ifNotFinite";

    explicit FunctionIfNotFinite(ContextPtr context_) : WithContext(context_) {}

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionIfNotFinite>(context_);
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto is_finite_type = FunctionFactory::instance().get("isFinite", getContext())->build({arguments[0]})->getResultType();
        auto if_type = FunctionFactory::instance().get("if", getContext())->build({{nullptr, is_finite_type, ""}, arguments[0], arguments[1]})->getResultType();
        return if_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName is_finite_columns{arguments[0]};
        auto is_finite = FunctionFactory::instance().get("isFinite", getContext())->build(is_finite_columns);
        auto res = is_finite->execute(is_finite_columns, is_finite->getResultType(), input_rows_count);

        ColumnsWithTypeAndName if_columns
        {
            {res, is_finite->getResultType(), ""},
            arguments[0],
            arguments[1],
        };

        auto func_if = FunctionFactory::instance().get("if", getContext())->build(if_columns);
        return func_if->execute(if_columns, result_type, input_rows_count);
    }
};

}

REGISTER_FUNCTION(IfNotFinite)
{
    factory.registerFunction<FunctionIfNotFinite>();
}

}

