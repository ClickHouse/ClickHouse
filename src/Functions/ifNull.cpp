#include <Functions/IFunctionImpl.h>
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

    explicit FunctionIfNull(const Context & context_) : context(context_) {}

    static FunctionPtr create(const Context & context)
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
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[1];

        if (!arguments[0]->isNullable())
            return arguments[0];

        return getLeastSupertype({removeNullable(arguments[0]), arguments[1]});
    }

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        /// Always null.
        if (columns[arguments[0]].type->onlyNull())
        {
            columns[result].column = columns[arguments[1]].column;
            return;
        }

        /// Could not contain nulls, so nullIf makes no sense.
        if (!columns[arguments[0]].type->isNullable())
        {
            columns[result].column = columns[arguments[0]].column;
            return;
        }

        /// ifNull(col1, col2) == if(isNotNull(col1), assumeNotNull(col1), col2)

        ColumnsWithTypeAndName temp_columns = columns;

        size_t is_not_null_pos = temp_columns.size();
        temp_columns.emplace_back(ColumnWithTypeAndName{nullptr, std::make_shared<DataTypeUInt8>(), ""});
        size_t assume_not_null_pos = temp_columns.size();
        temp_columns.emplace_back(ColumnWithTypeAndName{nullptr, removeNullable(columns[arguments[0]].type), ""});

        auto is_not_null = FunctionFactory::instance().get("isNotNull", context)->build(
            {temp_columns[arguments[0]]});

        auto assume_not_null = FunctionFactory::instance().get("assumeNotNull", context)->build(
            {temp_columns[arguments[0]]});

        auto func_if = FunctionFactory::instance().get("if", context)->build(
            {temp_columns[is_not_null_pos], temp_columns[assume_not_null_pos], temp_columns[arguments[1]]}); //-V557

        is_not_null->execute(temp_columns, {arguments[0]}, is_not_null_pos, input_rows_count);
        assume_not_null->execute(temp_columns, {arguments[0]}, assume_not_null_pos, input_rows_count);
        func_if->execute(temp_columns, {is_not_null_pos, assume_not_null_pos, arguments[1]}, result, input_rows_count);

        columns[result].column = std::move(temp_columns[result].column);
    }

private:
    const Context & context;
};

}

void registerFunctionIfNull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIfNull>(FunctionFactory::CaseInsensitive);
}

}
