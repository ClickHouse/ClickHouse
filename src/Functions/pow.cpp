#include <Functions/FunctionFactory.h>
#include <Functions/FunctionMathBinaryFloat64.h>
#include <Functions/IFunction.h>

#include <cmath>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/castColumn.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

struct PowName { static constexpr auto name = "pow"; };

/// `x * x` is the correctly rounded square, so `pow(x, 2)` needs no call of `std::pow`. Other exponents use `std::pow`,
/// whose results are correctly rounded in practice: exponentiation by squaring accumulates an error of up to tens of ulp.
/// Both specializations of the kernel promote the arguments to `Float64` first, so the `Float32` path of
/// `FunctionMathBinaryFloat64` produces the same values as the `Float64` path.
struct PowGeneralImpl
{
    static constexpr auto name = PowName::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T1, typename T2>
    static void execute(const T1 * src_left, const T2 * src_right, Float64 * dst)
    {
        const Float64 x = static_cast<Float64>(src_left[0]);
        const Float64 y = static_cast<Float64>(src_right[0]);
        dst[0] = y == 2 ? x * x : std::pow(x, y);
    }
};

/// `pow` with a fast path for the exponent 2. The kernel used for a row depends only on the argument values, never on
/// whether an argument is a `ColumnConst`, so `pow(b, 2)` and `pow(b, materialize(2))` are bit-identical. A constant
/// exponent 2 lets the whole column be squared in one vectorizable loop; every other shape is delegated to
/// `FunctionMathBinaryFloat64` with the row-wise kernel above, which keeps its in-place `ColumnConst` handling and
/// cast-free `Float32` specializations.
class FunctionPow final : public IFunction
{
public:
    static constexpr auto name = PowName::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPow>(); }

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto & arg : arguments)
            if (!isNativeNumber(arg) && !isDecimal(arg) && !WhichDataType(arg).isBFloat16())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                    arg->getName(), getName());
        return std::make_shared<DataTypeFloat64>();
    }

    /// Matches FunctionMathBinaryFloat64 so that pow(Dynamic, ...) returns Nullable(Float64) instead of Dynamic.
    [[nodiscard]] DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (isColumnConst(*arguments[1].column))
        {
            /// Cast the single constant, so the exponent value is exactly what the general path
            /// would see per row after its own cast.
            ColumnPtr exp_col = castColumn(arguments[1], std::make_shared<DataTypeFloat64>());
            const Float64 y = exp_col->getFloat64(0);
            if (y == 2)
                return executeSquare(arguments[0], input_rows_count);
        }

        return general->executeImpl(arguments, result_type, input_rows_count);
    }

private:
    template <typename T>
    static ColumnPtr executeSquareTyped(const PaddedPODArray<T> & base, size_t rows)
    {
        auto dst = ColumnFloat64::create(rows);
        auto & res = dst->getData();
        for (size_t i = 0; i < rows; ++i)
        {
            const Float64 x = static_cast<Float64>(base[i]);
            res[i] = x * x;
        }
        return dst;
    }

    /// Float columns are read in place; only other base types (integers, decimals, BFloat16) are cast to `Float64`
    /// first - the same promotion the row-wise kernel applies, so the values stay identical.
    static ColumnPtr executeSquare(const ColumnWithTypeAndName & base_arg, size_t rows)
    {
        if (const auto * base_f32 = checkAndGetColumn<ColumnFloat32>(base_arg.column.get()))
            return executeSquareTyped(base_f32->getData(), rows);
        if (const auto * base_f64 = checkAndGetColumn<ColumnFloat64>(base_arg.column.get()))
            return executeSquareTyped(base_f64->getData(), rows);

        ColumnPtr base_col = castColumn(base_arg, std::make_shared<DataTypeFloat64>());
        return executeSquareTyped(assert_cast<const ColumnFloat64 &>(*base_col).getData(), rows);
    }

    FunctionPtr general = FunctionMathBinaryFloat64<PowGeneralImpl>::create({});
};

}

REGISTER_FUNCTION(Pow)
{
    FunctionDocumentation::Description description = R"(
Returns x raised to the power of y.
)";
    FunctionDocumentation::Syntax syntax = "pow(x, y)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "The base.", {"(U)Int8/16/32/64", "Float*", "BFloat16", "Decimal*"}},
        {"y", "The exponent.", {"(U)Int8/16/32/64", "Float*", "BFloat16", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns x^y", {"Float64"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT pow(2, 3);", "8"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPow>(documentation, FunctionFactory::Case::Insensitive);
    factory.registerAlias("power", "pow", FunctionFactory::Case::Insensitive);
}

}
