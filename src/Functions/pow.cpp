#include <Functions/FunctionFactory.h>
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

/// `pow` with a fast path for integer exponents. The kernel used for a row depends only on the argument
/// values, never on whether an argument is a `ColumnConst`, so `pow(b, 17)` and `pow(b, materialize(17))`
/// are bit-identical and the function stays deterministic across plan rewrites and materialization:
///   - an integer exponent `n` with `|n| <= 64`: exponentiation by squaring (`pow(x, 2)` is one multiply);
///   - everything else: precise `std::pow`, so results stay correct across the whole domain
///     (negative bases, zero, NaN/Inf, subnormals).
/// A constant exponent only lets the whole column take the integer path in a vectorizable loop.
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto f64 = std::make_shared<DataTypeFloat64>();
        ColumnPtr base_col = castColumn(arguments[0], f64);
        ColumnPtr exp_col = castColumn(arguments[1], f64);

        if (isColumnConst(*exp_col))
        {
            const Float64 y = exp_col->getFloat64(0);
            if (isFastIntegerExponent(y))
                return executeIntegerExponent(base_col, static_cast<Int64>(y), input_rows_count);
        }

        return executeGeneral(base_col, exp_col, input_rows_count);
    }

private:
    static const PaddedPODArray<Float64> & fullData(ColumnPtr & holder, const ColumnPtr & col)
    {
        holder = col->convertToFullColumnIfConst();
        return assert_cast<const ColumnFloat64 &>(*holder).getData();
    }

    /// Squaring doubles the base's accumulated error, so accuracy decays like `|n| * eps`: measured
    /// 6.6e-15 at 64, 1.0e-13 at 1024. 64 keeps it around 1e-14. It also keeps the `Int64` cast
    /// below in range - `floor` alone admits 1e19.
    static constexpr Float64 max_fast_integer_exponent = 64;

    /// NaN and Inf are rejected: NaN != floor(NaN), and |Inf| > 64.
    static bool isFastIntegerExponent(Float64 y)
    {
        return y == std::floor(y) && std::abs(y) <= max_fast_integer_exponent;
    }

    /// Integer power by squaring. `pow(x, 0) == 1` for every x, including NaN and Inf (C/IEEE convention).
    /// For negative exponents the base is reciprocated up front rather than the final product inverted:
    /// `x^-n = (1/x)^n`. Inverting at the end would let the intermediate `x^n` overflow to +Inf (then
    /// `1/Inf == 0`), erasing results that are actually representable subnormals.
    static Float64 integerPow(Float64 base, Int64 n)
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

    /// The whole column takes the integer path; the specializations produce exactly what `integerPow` does
    /// (`1 * x == x`), they just let the loops vectorize.
    static ColumnPtr executeIntegerExponent(const ColumnPtr & base_col, Int64 n, size_t rows)
    {
        auto dst = ColumnFloat64::create(rows);
        auto & res = dst->getData();

        if (n == 0)
        {
            std::fill(res.begin(), res.end(), 1.0);
            return dst;
        }

        ColumnPtr base_holder;
        const auto & base = fullData(base_holder, base_col);

        if (n == 2)
        {
            for (size_t i = 0; i < rows; ++i)
                res[i] = base[i] * base[i];
            return dst;
        }

        for (size_t i = 0; i < rows; ++i)
            res[i] = integerPow(base[i], n);
        return dst;
    }

    /// Row-wise kernel selection by value.
    static ColumnPtr executeGeneral(const ColumnPtr & base_col, const ColumnPtr & exp_col, size_t rows)
    {
        ColumnPtr base_holder;
        ColumnPtr exp_holder;
        const auto & base = fullData(base_holder, base_col);
        const auto & exp = fullData(exp_holder, exp_col);

        auto dst = ColumnFloat64::create(rows);
        auto & res = dst->getData();
        for (size_t i = 0; i < rows; ++i)
        {
            const Float64 y = exp[i];
            res[i] = isFastIntegerExponent(y) ? integerPow(base[i], static_cast<Int64>(y)) : std::pow(base[i], y);
        }
        return dst;
    }
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
