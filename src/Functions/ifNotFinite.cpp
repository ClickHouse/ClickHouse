#include <Functions/IFunctionImpl.h>
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
class FunctionIfNotFinite : public IFunction
{
public:
    static constexpr auto name = "ifNotFinite";

    explicit FunctionIfNotFinite(const Context & context_) : context(context_) {}

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionIfNotFinite>(context);
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto is_finite_type = FunctionFactory::instance().get("isFinite", context)->build({arguments[0]})->getReturnType();
        auto if_type = FunctionFactory::instance().get("if", context)->build({{nullptr, is_finite_type, ""}, arguments[0], arguments[1]})->getReturnType();
        return if_type;
    }

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName temp_columns = columns;

        auto is_finite = FunctionFactory::instance().get("isFinite", context)->build({temp_columns[arguments[0]]});

        size_t is_finite_pos = temp_columns.size();
        temp_columns.emplace_back(ColumnWithTypeAndName{nullptr, is_finite->getReturnType(), ""});

        auto func_if = FunctionFactory::instance().get("if", context)->build(
            {temp_columns[is_finite_pos], temp_columns[arguments[0]], temp_columns[arguments[1]]});

        is_finite->execute(temp_columns, {arguments[0]}, is_finite_pos, input_rows_count);

        func_if->execute(temp_columns, {is_finite_pos, arguments[0], arguments[1]}, result, input_rows_count);

        columns[result].column = std::move(temp_columns[result].column);
    }

private:
    const Context & context;
};

}

void registerFunctionIfNotFinite(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIfNotFinite>();
}

}

