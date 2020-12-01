#include <Functions/IFunctionImpl.h>
#include <Functions/castTypeToEither.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

/// Returns 1 if argument is zero or NULL.
/// It can be used to negate filter in WHERE condition.
/// "WHERE isZeroOrNull(expr)" will return exactly the same rows that "WHERE expr" will filter out.
class FunctionIsZeroOrNull : public IFunction
{
public:
    static constexpr auto name = "isZeroOrNull";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIsZeroOrNull>();
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & types) const override
    {
        if (!isNumber(removeNullable(types.at(0))))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of function {} must have simple numeric type, possibly Nullable", name);

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const ColumnPtr & input_column = block.getByPosition(arguments[0]).column;

        if (const ColumnNullable * input_column_nullable = checkAndGetColumn<ColumnNullable>(input_column.get()))
        {
            const NullMap & null_map = input_column_nullable->getNullMapData();
            const IColumn * nested_column = &input_column_nullable->getNestedColumn();

            if (!castTypeToEither<
                ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64,
                ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
                ColumnFloat32, ColumnFloat64>(
                nested_column, [&](const auto & column)
                {
                    auto res = ColumnUInt8::create(input_rows_count);
                    processNullable(column.getData(), null_map, res->getData(), input_rows_count);
                    block.getByPosition(result).column = std::move(res);
                    return true;
                }))
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The argument of function {} must have simple numeric type, possibly Nullable", name);
            }
        }
        else
        {
            if (!castTypeToEither<
                ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64,
                ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
                ColumnFloat32, ColumnFloat64>(
                input_column.get(), [&](const auto & column)
                {
                    auto res = ColumnUInt8::create(input_rows_count);
                    processNotNullable(column.getData(), res->getData(), input_rows_count);
                    block.getByPosition(result).column = std::move(res);
                    return true;
                }))
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The argument of function {} must have simple numeric type, possibly Nullable", name);
            }
        }
    }

private:
    template <typename InputData>
    void processNotNullable(const InputData & input_data, ColumnUInt8::Container & result_data, size_t input_rows_count) const
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            result_data[i] = !input_data[i];
    }

    template <typename InputData>
    void processNullable(const InputData & input_data, const NullMap & input_null_map,
        ColumnUInt8::Container & result_data, size_t input_rows_count) const
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            result_data[i] = input_null_map[i] || !input_data[i];
    }
};


void registerFunctionIsZeroOrNull(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsZeroOrNull>();
}

}

