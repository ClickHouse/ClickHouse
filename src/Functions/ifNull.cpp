#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>


namespace DB
{
namespace
{

/// Implements the function ifNull which takes 2 arguments and returns
/// the value of the 1st argument if it is not null. Otherwise it returns
/// the value of the 2nd argument.
class FunctionIfNull : public IFunction
{
public:
    static constexpr auto name = "ifNull";

    explicit FunctionIfNull(ContextPtr context_) : context(context_) {}

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionIfNull>(context);
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[1];

        if (!arguments[0]->isNullable())
            return arguments[0];

        return getLeastSupertype(DataTypes{removeNullable(arguments[0]), arguments[1]});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Always null.
        if (arguments[0].type->onlyNull())
            return arguments[1].column;

        /// Could not contain nulls, so nullIf makes no sense.
        if (!arguments[0].type->isNullable())
            return arguments[0].column;

        /// ifNull(col1, col2) == if(isNotNull(col1), assumeNotNull(col1), col2)

        ColumnsWithTypeAndName columns{arguments[0]};

        auto is_not_null = FunctionFactory::instance().get("isNotNull", context)->build(columns);
        auto is_not_null_type = std::make_shared<DataTypeUInt8>();
        auto is_not_null_res = is_not_null->execute(columns, is_not_null_type, input_rows_count);

        auto assume_not_null = FunctionFactory::instance().get("assumeNotNull", context)->build(columns);
        auto assume_not_null_type = removeNullable(arguments[0].type);
        auto assume_nut_null_res = assume_not_null->execute(columns, assume_not_null_type, input_rows_count);

        ColumnsWithTypeAndName if_columns
        {
                {is_not_null_res, is_not_null_type, ""},
                {assume_nut_null_res, assume_not_null_type, ""},
                arguments[1],
        };

        auto func_if = FunctionFactory::instance().get("if", context)->build(if_columns);
        return func_if->execute(if_columns, result_type, input_rows_count);
    }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(IfNull)
{
    factory.registerFunction<FunctionIfNull>({}, FunctionFactory::Case::Insensitive);
}

}
