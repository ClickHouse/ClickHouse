#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnConst.h>
#include <Common/intExp.h>


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

/// Returns 1 if and Decimal value has more digits then it's Precision allow, 0 otherwise.
/// Precision could be set as second argument or omitted. If omitted function uses Decimal precision of the first argument.
class FunctionIsDecimalOverflow : public IFunction
{
public:
    static constexpr auto name = "isDecimalOverflow";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIsDecimalOverflow>();
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

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

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using Type = typename Types::RightType;
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
        if (!callOnBasicType<void, false, false, true, false>(dec_type_idx, call))
            throw Exception("Wrong call for " + getName() + " with " + src_column.type->getName(),
                            ErrorCodes::ILLEGAL_COLUMN);

        return result_column;
    }

private:
    template <typename T>
    static void execute(const ColumnDecimal<T> & col, ColumnUInt8 & result_column, size_t rows_count, UInt32 precision)
    {
        const auto & src_data = col.getData();
        auto & dst_data = result_column.getData();
        dst_data.resize(rows_count);

        for (size_t i = 0; i < rows_count; ++i)
            dst_data[i] = outOfDigits<T>(src_data[i], precision);
    }

    template <typename T>
    static bool outOfDigits(T dec, UInt32 precision)
    {
        static_assert(IsDecimalNumber<T>);
        using NativeT = typename T::NativeType;

        if (precision > DecimalUtils::maxPrecision<T>())
            return false;

        NativeT pow10 = intExp10OfSize<NativeT>(precision);

        if (dec.value < 0)
            return dec.value <= -pow10;
        return dec.value >= pow10;
    }
};

}

void registerFunctionIsDecimalOverflow(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIsDecimalOverflow>();
}

}
