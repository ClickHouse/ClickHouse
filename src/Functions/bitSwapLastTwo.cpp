#include <Functions/FunctionFactory.h>
#include <Functions/FunctionUnaryArithmetic.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Working with UInt8: last bit = can be true, previous = can be false (Like src/Storages/MergeTree/BoolMask.h).
/// This function provides "NOT" operation for BoolMasks by swapping last two bits ("can be true" <-> "can be false").
template <typename A>
struct BitSwapLastTwoImpl
{
    using ResultType = UInt8;
    static constexpr const bool allow_string_or_fixed_string = false;

    static ResultType NO_SANITIZE_UNDEFINED apply([[maybe_unused]] A a)
    {
        /// Other argument types are rejected by `getReturnTypeImpl`.
        if constexpr (!std::is_same_v<A, ResultType>)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "It's a bug! Only UInt8 type is supported by __bitSwapLastTwo.");

        auto little_bits = littleBits<A>(a);
        return static_cast<ResultType>(((little_bits & 1) << 1) | ((little_bits >> 1) & 1));
    }

#if USE_EMBEDDED_COMPILER
static constexpr bool compilable = true;

static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * arg, bool)
{
    if (!arg->getType()->isIntegerTy())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "__bitSwapLastTwo expected an integral type");
    return b.CreateOr(
            b.CreateShl(b.CreateAnd(arg, 1), 1),
            b.CreateAnd(b.CreateLShr(arg, 1), 1)
            );
}
#endif
};

struct NameBitSwapLastTwo { static constexpr auto name = "__bitSwapLastTwo"; };

/// The result of this function is always UInt8 regardless of the argument type.
/// Override `getReturnTypeForDefaultImplementationForDynamic` so that Dynamic arguments
/// produce Nullable(UInt8) instead of Dynamic.
class FunctionBitSwapLastTwo final : public FunctionUnaryArithmetic<BitSwapLastTwoImpl, NameBitSwapLastTwo, false>
{
public:
    using FunctionUnaryArithmetic::FunctionUnaryArithmetic;

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionBitSwapLastTwo>(context_); }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isUInt8(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());
        return FunctionUnaryArithmetic::getReturnTypeImpl(arguments);
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }
};

}

template <> struct FunctionUnaryArithmeticMonotonicity<NameBitSwapLastTwo>
{
    static bool has() { return false; }
    static IFunction::Monotonicity get(const IDataType &, const Field &, const Field &)
    {
        return {};
    }
};

REGISTER_FUNCTION(BitSwapLastTwo)
{
    factory.registerFunction<FunctionBitSwapLastTwo>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
}

}
