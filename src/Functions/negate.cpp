#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
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

        return -static_cast<ResultType>(a);
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
        if (const DataTypeNullable * t = typeid_cast<const DataTypeNullable *>(type))
            type = t->getNestedType().get();

        /// If the input is signed, assume monotonic.
        /// Not fully correct because of the corner case -INT64_MIN == INT64_MIN and similar.
        if (!type->isValueRepresentedByUnsignedInteger())
            return { .is_monotonic = true, .is_positive = false, .is_strict = true };

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
                case Field::Types::UInt64: return f.safeGet<UInt64>() > 1ul << 63;
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
    FunctionDocumentation::ReturnedValue returned_value = "Returns -x from x";
    FunctionDocumentation::Example example1 = {"Usage example", "SELECT negate(10)", "-10"};
    FunctionDocumentation::Examples examples = {example1};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category categories = FunctionDocumentation::Category::Arithmetic;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, categories};

    factory.registerFunction<FunctionNegate>(documentation);
}

}
