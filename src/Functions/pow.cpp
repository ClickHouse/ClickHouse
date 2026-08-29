#include <Functions/FunctionMathBinaryFloat64.h>
#include <Functions/FunctionFactory.h>

#include "config.h"

#if USE_FASTOPS
#include <cmath>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/castColumn.h>
#include <Common/assert_cast.h>
#include <fastops/fastops.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

struct PowName { static constexpr auto name = "pow"; };

#if USE_FASTOPS

/// Fast, vectorized `pow` for the common special cases:
///   - constant integer exponent (including 0, 1, 2, and negatives) via multiplication;
///   - constant positive base `b`, evaluated as `b^y = exp2(y * log2(b))`.
/// Anything else (non-integer constant exponent, non-positive constant base, or two non-constant
/// arguments) falls back to precise scalar `std::pow`, so results stay correct across the whole
/// domain (negative bases, zero, NaN/Inf, subnormals).
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
            if (auto res = executeConstExponent(base_col, y, input_rows_count))
                return res;
        }

        if (isColumnConst(*base_col))
        {
            const Float64 b = base_col->getFloat64(0);
            if (auto res = executeConstBase(b, exp_col, input_rows_count))
                return res;
        }

        return executePrecise(base_col, exp_col, input_rows_count);
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

    /// pow(x, y) with a constant exponent. Only the integer-exponent cases are fast-pathed;
    /// a non-integer constant exponent returns nullptr so the caller falls back to precise pow.
    static ColumnPtr executeConstExponent(const ColumnPtr & base_col, Float64 y, size_t rows)
    {
        if (y != std::floor(y) || std::abs(y) > max_fast_integer_exponent)
            return nullptr;

        auto dst = ColumnFloat64::create(rows);
        auto & res = dst->getData();
        const auto n = static_cast<Int64>(y);

        /// pow(x, 0) == 1 for every x, including NaN and Inf (C/IEEE convention); the base is never read.
        if (n == 0)
        {
            std::fill(res.begin(), res.end(), 1.0);
            return dst;
        }

        ColumnPtr base_holder;
        const auto & base = fullData(base_holder, base_col);

        /// Squaring is the common case and vectorizes to a single multiply per row.
        if (n == 2)
        {
            for (size_t i = 0; i < rows; ++i)
                res[i] = base[i] * base[i];
            return dst;
        }

        /// General integer power by squaring (handles n == 1, n >= 3, and negative n).
        /// For negative exponents we reciprocate the base up front rather than inverting the final
        /// product: `x^-n = (1/x)^n`. Inverting at the end would let the intermediate `x^n` overflow
        /// to +Inf (then `1/Inf == 0`), erasing results that are actually representable subnormals.
        const bool negative = n < 0;
        auto exponent = static_cast<UInt64>(negative ? -n : n);
        for (size_t i = 0; i < rows; ++i)
        {
            Float64 acc = 1.0;
            Float64 b = negative ? 1.0 / base[i] : base[i];
            UInt64 e = exponent;
            while (e != 0)
            {
                if (e & 1u)
                    acc *= b;
                b *= b;
                e >>= 1u;
            }
            res[i] = acc;
        }
        return dst;
    }

    /// pow(b, y) with a constant base. Fast-pathed only for a positive finite base, where
    /// b^y = exp2(y * log2(b)) is well defined for all y (the result never becomes NaN from a
    /// negative base). Returns nullptr otherwise.
    static ColumnPtr executeConstBase(Float64 b, const ColumnPtr & exp_col, size_t rows)
    {
        if (b == 1.0)
        {
            /// pow(1, y) == 1 for every y, including NaN (C/IEEE convention).
            auto dst = ColumnFloat64::create(rows, 1.0);
            return dst;
        }

        if (!(b > 0.0) || !std::isfinite(b))
            return nullptr;

        ColumnPtr exp_holder;
        const auto & exp = fullData(exp_holder, exp_col);

        auto dst = ColumnFloat64::create(rows);
        auto & res = dst->getData();

        const Float64 log2_b = std::log2(b);
        for (size_t i = 0; i < rows; ++i)
            res[i] = exp[i] * log2_b;

        NFastOps::Exp2<true>(res.data(), rows, res.data());
        return dst;
    }

    static ColumnPtr executePrecise(const ColumnPtr & base_col, const ColumnPtr & exp_col, size_t rows)
    {
        ColumnPtr base_holder;
        ColumnPtr exp_holder;
        const auto & base = fullData(base_holder, base_col);
        const auto & exp = fullData(exp_holder, exp_col);

        auto dst = ColumnFloat64::create(rows);
        auto & res = dst->getData();
        for (size_t i = 0; i < rows; ++i)
            res[i] = std::pow(base[i], exp[i]);
        return dst;
    }
};

#else
using FunctionPow = FunctionMathBinaryFloat64<BinaryFunctionVectorized<PowName, pow>>;
#endif

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
