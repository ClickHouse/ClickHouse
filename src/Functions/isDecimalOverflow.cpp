#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnConst.h>
#include <Common/intExp.h>
#include <base/TypePair.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Returns 1 if Decimal value has more digits than its Precision allows, 0 otherwise.
/// Precision could be set as second argument or omitted.
/// If omitted, function uses Decimal precision of the first argument.
class FunctionIsDecimalOverflow : public IFunction
{
public:
    static constexpr auto name = "isDecimalOverflow";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionIsDecimalOverflow>();
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " +
                toString(arguments.size()) + ", should be 1 or 2.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        WhichDataType which_first(arguments[0]->getTypeId());

        if (!which_first.isDecimal())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2)
        {
            WhichDataType which_second(arguments[1]->getTypeId());
            if (!which_second.isUInt8())
                throw Exception("Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeUInt8>();
    }

private:
    template <Decimal T>
    static constexpr bool outOfDigits(const T& decimal, UInt32 precision)
    {
        using NativeT = typename T::NativeType;

        if (precision > DecimalUtils::max_precision<T>)
            return false;

        const NativeT pow10 = intExp10OfSize<NativeT>(precision);

        if (decimal.value < 0)
            return decimal.value <= -pow10;
        return decimal.value >= pow10;
    }

    template <typename T>
    static void execute(const ColumnDecimal<T> & col, ColumnUInt8 & result_column, size_t rows_count, UInt32 precision)
    {
        const auto & src_data = col.getData();
        auto & dst_data = result_column.getData();
        dst_data.resize(rows_count);

        for (size_t i = 0; i < rows_count; ++i)
            dst_data[i] = outOfDigits<T>(src_data[i], precision);
    }

public:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & src_column = arguments[0];
        if (!src_column.column)
            throw Exception("Illegal column while execute function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        UInt32 precision = 0;
        if (arguments.size() == 2)
        {
            const auto & precision_column = arguments[1];
            if (!precision_column.column)
                throw Exception("Illegal column while execute function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const ColumnConst * const_column = checkAndGetColumnConst<ColumnUInt8>(precision_column.column.get());
            if (!const_column)
                throw Exception("Second argument for function " + getName() + " must be constant UInt8: precision.",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            precision = const_column->getValue<UInt8>();
        }
        else
            precision = getDecimalPrecision(*src_column.type);

        auto result_column = ColumnUInt8::create();

        auto call = [&]<class Type>(TypePair<void, Type>)
        {
            using ColVecType = ColumnDecimal<Type>;

            if (const ColumnConst * const_column = checkAndGetColumnConst<ColVecType>(src_column.column.get()))
            {
                Type const_decimal = checkAndGetColumn<ColVecType>(const_column->getDataColumnPtr().get())->getData()[0];
                UInt8 res_value = outOfDigits<Type>(const_decimal, precision);
                result_column->getData().resize_fill(input_rows_count, res_value);
                return true;
            }
            else if (const ColVecType * col_vec = checkAndGetColumn<ColVecType>(src_column.column.get()))
            {
                execute<Type>(*col_vec, *result_column, input_rows_count, precision);
                return true;
            }

            throw Exception("Illegal column while execute function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        };

        TypeIndex dec_type_idx = src_column.type->getTypeId();

        if (!dispatchOverType<Dispatch{ .decimals = true }>(dec_type_idx, std::move(call)))
            throw Exception("Wrong call for " + getName() + " with " + src_column.type->getName(),
                            ErrorCodes::ILLEGAL_COLUMN);

        return result_column;
    }
};

}

void registerFunctionIsDecimalOverflow(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsDecimalOverflow>();
}

}
