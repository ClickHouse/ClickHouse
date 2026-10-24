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

/// Squaring doubles the base's accumulated error, so accuracy decays like `|n| * eps`: measured
/// 6.6e-15 at 64, 1.0e-13 at 1024. 64 keeps it around 1e-14. It also keeps the `Int64` cast
/// below in range - `floor` alone admits 1e19.
constexpr Float64 max_fast_integer_exponent = 64;

/// NaN and Inf are rejected: NaN != floor(NaN), and |Inf| > 64.
bool isFastIntegerExponent(Float64 y)
{
    return y == std::floor(y) && std::abs(y) <= max_fast_integer_exponent;
}

/// Integer power by squaring. `pow(x, 0) == 1` for every x, including NaN and Inf (C/IEEE convention).
/// For negative exponents the base is reciprocated up front rather than the final product inverted:
/// `x^-n = (1/x)^n`. Inverting at the end would let the intermediate `x^n` overflow to +Inf (then
/// `1/Inf == 0`), erasing results that are actually representable subnormals.
Float64 integerPow(Float64 base, Int64 n)
{
    Float64 acc = 1.0;
    Float64 b = n < 0 ? 1.0 / base : base;
    auto e = static_cast<UInt64>(n < 0 ? -n : n);
    while (e != 0)
    {
        if (e & 1u)
            acc *= b;
        b *= b;
        e >>= 1u;
    }
    return acc;
}

/// Row-wise kernel for the general case: the kernel is selected by the exponent value, so the result
/// is bit-identical to the whole-column integer fast path below. Both specializations of the kernel
/// promote the arguments to `Float64` first, so the `Float32` path of `FunctionMathBinaryFloat64`
/// produces the same values as the `Float64` path.
struct PowGeneralImpl
{
    static constexpr auto name = PowName::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T1, typename T2>
    static void execute(const T1 * src_left, const T2 * src_right, Float64 * dst)
    {
        const Float64 x = static_cast<Float64>(src_left[0]);
        const Float64 y = static_cast<Float64>(src_right[0]);
        dst[0] = isFastIntegerExponent(y) ? integerPow(x, static_cast<Int64>(y)) : std::pow(x, y);
    }
};

/// `pow` with a fast path for integer exponents. The kernel used for a row depends only on the argument
/// values, never on whether an argument is a `ColumnConst`, so `pow(b, 17)` and `pow(b, materialize(17))`
/// are bit-identical and the function stays deterministic across plan rewrites and materialization:
///   - an integer exponent `n` with `|n| <= 64`: exponentiation by squaring (`pow(x, 2)` is one multiply);
///   - everything else: precise `std::pow`, so results stay correct across the whole domain
///     (negative bases, zero, NaN/Inf, subnormals).
/// A constant exponent only lets the whole column take the integer path in one vectorizable loop; every
/// other shape is delegated to `FunctionMathBinaryFloat64` with the row-wise kernel above, which keeps
/// its in-place `ColumnConst` handling and cast-free `Float32` specializations.
class FunctionPow final : public IFunction
{
public:
    static constexpr auto name = PowName::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPow>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto & arg : arguments)
            if (!isNativeNumber(arg) && !isDecimal(arg) && !WhichDataType(arg).isBFloat16())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                    arg->getName(), getName());
        return std::make_shared<DataTypeFloat64>();
    }

    /// Matches FunctionMathBinaryFloat64 so that pow(Dynamic, ...) returns Nullable(Float64) instead of Dynamic.
    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (isColumnConst(*arguments[1].column))
        {
            /// Cast the single constant, so the exponent value is exactly what the general path
            /// would see per row after its own cast.
            ColumnPtr exp_col = castColumn(arguments[1], std::make_shared<DataTypeFloat64>());
            const Float64 y = exp_col->getFloat64(0);
            if (isFastIntegerExponent(y))
                return executeIntegerExponent(arguments[0], static_cast<Int64>(y), input_rows_count);
        }

        return general->executeImpl(arguments, result_type, input_rows_count);
    }

private:
    template <typename T>
    static ColumnPtr executeIntegerExponentTyped(const PaddedPODArray<T> & base, Int64 n, size_t rows)
    {
        auto dst = ColumnFloat64::create(rows);
        auto & res = dst->getData();

        /// Produces exactly what `integerPow` does (`1 * x == x`), it just lets the loop vectorize.
        if (n == 2)
        {
            for (size_t i = 0; i < rows; ++i)
            {
                const Float64 x = static_cast<Float64>(base[i]);
                res[i] = x * x;
            }
            return dst;
        }

        for (size_t i = 0; i < rows; ++i)
            res[i] = integerPow(static_cast<Float64>(base[i]), n);
        return dst;
    }

    /// The whole column takes the integer path. Float columns are read in place; only other base types
    /// (integers, decimals, BFloat16) are cast to `Float64` first - the same promotion the row-wise
    /// kernel applies, so the values stay identical.
    static ColumnPtr executeIntegerExponent(const ColumnWithTypeAndName & base_arg, Int64 n, size_t rows)
    {
        if (n == 0)
        {
            auto dst = ColumnFloat64::create(rows);
            std::fill(dst->getData().begin(), dst->getData().end(), 1.0);
            return dst;
        }

        if (const auto * base_f32 = checkAndGetColumn<ColumnFloat32>(base_arg.column.get()))
            return executeIntegerExponentTyped(base_f32->getData(), n, rows);
        if (const auto * base_f64 = checkAndGetColumn<ColumnFloat64>(base_arg.column.get()))
            return executeIntegerExponentTyped(base_f64->getData(), n, rows);

        ColumnPtr base_col = castColumn(base_arg, std::make_shared<DataTypeFloat64>());
        return executeIntegerExponentTyped(assert_cast<const ColumnFloat64 &>(*base_col).getData(), n, rows);
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
