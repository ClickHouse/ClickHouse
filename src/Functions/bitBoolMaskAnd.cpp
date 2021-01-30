#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <DataTypes/NumberTraits.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Working with UInt8: last bit = can be true, previous = can be false (Like src/Storages/MergeTree/BoolMask.h).
/// This function provides "AND" operation for BoolMasks.
/// Returns: "can be true" = A."can be true" AND B."can be true"
///          "can be false" = A."can be false" OR B."can be false"
template <typename A, typename B>
struct BitBoolMaskAndImpl
{
    using ResultType = UInt8;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    static inline Result apply([[maybe_unused]] A left, [[maybe_unused]] B right)
    {
        // Should be a logical error, but this function is callable from SQL.
        // Need to investigate this.
        if constexpr (!std::is_same_v<A, ResultType> || !std::is_same_v<B, ResultType>)
            throw DB::Exception("It's a bug! Only UInt8 type is supported by __bitBoolMaskAnd.", ErrorCodes::BAD_ARGUMENTS);

        auto left_bits = littleBits<A>(left);
        auto right_bits = littleBits<B>(right);
        return static_cast<ResultType>((left_bits & right_bits & 1) | ((((left_bits >> 1) | (right_bits >> 1)) & 1) << 1));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameBitBoolMaskAnd { static constexpr auto name = "__bitBoolMaskAnd"; };
using FunctionBitBoolMaskAnd = BinaryArithmeticOverloadResolver<BitBoolMaskAndImpl, NameBitBoolMaskAnd>;

}

void registerFunctionBitBoolMaskAnd(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitBoolMaskAnd>();
}

}
