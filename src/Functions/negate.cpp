#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/NumberTraits.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

template <typename A>
struct NegateImpl
{
    using ResultType = std::conditional_t<is_decimal<A>, A, typename NumberTraits::ResultOfNegate<A>::Type>;
    static constexpr const bool allow_string_or_fixed_string = false;

    static NO_SANITIZE_UNDEFINED ResultType apply(A a)
    {
        if constexpr (is_decimal<A>)
            return negateOverflow(a);

        return static_cast<ResultType>(-static_cast<ResultType>(a));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
    {
        return arg->getType()->isIntegerTy() ? b.CreateNeg(arg) : b.CreateFNeg(arg);
    }
#endif
};

struct NameNegate { static constexpr auto name = "negate"; };
using FunctionNegate = FunctionUnaryArithmetic<NegateImpl, NameNegate, true>;

template <> struct FunctionUnaryArithmeticMonotonicity<NameNegate>
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const IDataType & original_type, const Field & left, const Field & right)
    {
        const IDataType * type = &original_type;
        if (const DataTypeLowCardinality * t = typeid_cast<const DataTypeLowCardinality *>(type))
            type = t->getDictionaryType().get();
        bool is_nullable = false;
        if (const DataTypeNullable * t = typeid_cast<const DataTypeNullable *>(type))
        {
            is_nullable = true;
            type = t->getNestedType().get();
        }

        /// For compound types (Tuple, Array, etc.) monotonicity analysis is not applicable.
        if (!type->isValueRepresentedByNumber())
            return {};

        /// `negate(NULL) = NULL` stays at the bottom of the sort order instead of
        /// flipping to the top, so negate is not monotonic on ranges that include NULL.
        /// For Nullable types, a null left bound indicates the range starts from NULL.
        if (is_nullable && left.isNull())
            return {};

        if (!type->isValueRepresentedByUnsignedInteger())
        {
            /// For signed integers, `negate(TYPE_MIN)` overflows to TYPE_MIN, breaking monotonicity.
            /// Only claim monotonic if the range provably excludes the type minimum.
            if (type->isValueRepresentedByInteger())
            {
                /// -Inf means the range starts from the very minimum, which is TYPE_MIN.
                if (left.isNegativeInfinity())
                    return {};

                /// Check if the left bound equals the type's minimum value.
                bool left_is_type_min = false;
                WhichDataType which(*type);
                if (left.getType() == Field::Types::Int64)
                {
                    Int64 v = left.safeGet<Int64>();
                    if (which.isInt8())
                        left_is_type_min = (v == std::numeric_limits<Int8>::min());
                    else if (which.isInt16())
                        left_is_type_min = (v == std::numeric_limits<Int16>::min());
                    else if (which.isInt32())
                        left_is_type_min = (v == std::numeric_limits<Int32>::min());
                    else if (which.isInt64())
                        left_is_type_min = (v == std::numeric_limits<Int64>::min());
                }
                else if (left.getType() == Field::Types::Int128)
                    left_is_type_min = (left.safeGet<Int128>() == std::numeric_limits<Int128>::min());
                else if (left.getType() == Field::Types::Int256)
                    left_is_type_min = (left.safeGet<Int256>() == std::numeric_limits<Int256>::min());

                if (left_is_type_min)
                    return {};
            }

            /// For floating-point types, `negate(NaN) = NaN` stays at the same sort
            /// position instead of flipping to the top, breaking monotonicity.
            if (!type->isValueRepresentedByInteger())
            {
                /// Infinity means the range could include NaN (which sorts at the
                /// extremes depending on `nan_direction_hint`).
                if (left.isNegativeInfinity() || right.isPositiveInfinity())
                    return {};

                /// Check actual bound values for NaN.
                auto isNaNField = [](const Field & f) -> bool
                {
                    if (f.isNull())
                        return false;
                    return std::isnan(applyVisitor(FieldVisitorConvertToNumber<Float64>(), f));
                };
                if (isNaNField(left) || isNaNField(right))
                    return {};
            }
            return { .is_monotonic = true, .is_positive = false, .is_strict = true };
        }

        /// For unsigned integers, negate overflows at the midpoint (e.g. 2^63 for UInt64).
        if (left.isNull())
            return {};

        /// negate(UInt64) -> Int64:
        ///  * monotonically decreases on [0, 2^63] (no overflow),
        ///  * then jumps up from -2^63 to 2^63-1, then
        ///  * monotonically decreases on [2^63+1, 2^64-1] (with overflow).
        /// Similarly for UInt128 and UInt256.
        /// Note: we currently don't handle the corner case -UINT64_MIN == UINT64_MIN.

        auto will_overflow = [](const Field & f, bool if_null) -> bool
        {
            switch (f.getType())
            {
                case Field::Types::UInt64: return f.safeGet<UInt64>() > UInt64(1) << 63;
                case Field::Types::UInt128: return f.safeGet<UInt128>() > UInt128(1) << 127;
                case Field::Types::UInt256: return f.safeGet<UInt256>() > UInt256(1) << 255;
                default: break;
            }
            if (f.isPositiveInfinity())
                return true;
            if (f.isNegativeInfinity())
                return false;
            if (f.isNull())
                return if_null;

            return if_null; // likely unreachable
        };

        bool is_monotonic = will_overflow(left, false) == will_overflow(right, true);
        return { .is_monotonic = is_monotonic, .is_positive = false, .is_strict = true };
    }
};

REGISTER_FUNCTION(Negate)
{
    FunctionDocumentation::Description description = "Negates the argument `x`. The result is always signed.";
    FunctionDocumentation::Syntax syntax = "negate(x)";
    FunctionDocumentation::Argument argument1 = {"x", "The value to negate."};
    FunctionDocumentation::Arguments arguments = {argument1};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns -x from x"};
    FunctionDocumentation::Example example1 = {"Usage example", "SELECT negate(10)", "-10"};
    FunctionDocumentation::Examples examples = {example1};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNegate>(documentation);
}

}
