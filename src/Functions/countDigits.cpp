#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <base/extended_types.h>
#include <base/itoa.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

template <typename T>
int digits10(T x)
{
    if (x < 10ULL)
        return 1;
    if (x < 100ULL)
        return 2;
    if (x < 1000ULL)
        return 3;

    if (x < 1000000000000ULL)
    {
        if (x < 100000000ULL)
        {
            if (x < 1000000ULL)
            {
                if (x < 10000ULL)
                    return 4;
                return 5 + (x >= 100000ULL);
            }

            return 7 + (x >= 10000000ULL);
        }

        if (x < 10000000000ULL)
            return 9 + (x >= 1000000000ULL);

        return 11 + (x >= 100000000000ULL);
    }

    return 12 + digits10(x / 1000000000000ULL);
}

/// Returns number of decimal digits you need to represent the value.
/// For Decimal values takes in account their scales: calculates result over underlying int type which is (value * scale).
/// countDigits(42) = 2, countDigits(42.000) = 5, countDigits(0.04200) = 4.
/// I.e. you may check decimal overflow for Decimal64 with 'countDecimal(x) > 18'. It's a slow variant of isDecimalOverflow().
class FunctionCountDigits : public IFunction
{
public:
    static constexpr auto name = "countDigits";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionCountDigits>();
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType which_first(arguments[0]->getTypeId());

        if (!which_first.isInt() && !which_first.isUInt() && !which_first.isDecimal())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                            arguments[0]->getName(), getName());

        return std::make_shared<DataTypeUInt8>(); /// Up to 255 decimal digits.
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & src_column = arguments[0];
        if (!src_column.column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());

        auto result_column = ColumnUInt8::create();

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using Type = typename Types::RightType;
            using ColVecType = ColumnVectorOrDecimal<Type>;

            if (const ColVecType * col_vec = checkAndGetColumn<ColVecType>(src_column.column.get()))
            {
                execute<Type>(*col_vec, *result_column, input_rows_count);
                return true;
            }

            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());
        };

        TypeIndex dec_type_idx = src_column.type->getTypeId();
        if (!callOnBasicType<void, true, false, true, false>(dec_type_idx, call))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Wrong call for {} with {}", getName(), src_column.type->getName());

        return result_column;
    }

private:
    template <typename T, typename ColVecType>
    static void execute(const ColVecType & col, ColumnUInt8 & result_column, size_t rows_count)
    {
        using NativeT = make_unsigned_t<NativeType<T>>;

        const auto & src_data = col.getData();
        auto & dst_data = result_column.getData();
        dst_data.resize(rows_count);

        for (size_t i = 0; i < rows_count; ++i)
        {
            if constexpr (is_decimal<T>)
            {
                auto value = src_data[i].value;
                if (unlikely(value < 0))
                    dst_data[i] = digits10<NativeT>(-static_cast<NativeT>(value));
                else
                    dst_data[i] = digits10<NativeT>(value);
            }
            else
            {
                auto value = src_data[i];
                if (unlikely(value < 0))
                    dst_data[i] = digits10<NativeT>(-static_cast<NativeT>(value));
                else
                    dst_data[i] = digits10<NativeT>(value);
            }
        }
    }
};

}

REGISTER_FUNCTION(CountDigits)
{
    factory.registerFunction<FunctionCountDigits>();
}

}
