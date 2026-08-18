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
/// This function provides "OR" operation for BoolMasks.
/// Returns: "can be true" = A."can be true" OR B."can be true"
///          "can be false" = A."can be false" AND B."can be false"
template <typename A, typename B>
struct BitBoolMaskOrImpl
{
    using ResultType = UInt8;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply([[maybe_unused]] A left, [[maybe_unused]] B right)
    {
        if constexpr (!std::is_same_v<A, ResultType> || !std::is_same_v<B, ResultType>)
            // Should be a logical error, but this function is callable from SQL.
            // Need to investigate this.
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "It's a bug! Only UInt8 type is supported by __bitBoolMaskOr.");

        auto left_bits = littleBits<A>(left);
        auto right_bits = littleBits<B>(right);
        return static_cast<ResultType>(((left_bits | right_bits) & 1) | ((((left_bits >> 1) & (right_bits >> 1)) & 1) << 1));
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameBitBoolMaskOr { static constexpr auto name = "__bitBoolMaskOr"; };
using FunctionBitBoolMaskOr = BinaryArithmeticOverloadResolver<BitBoolMaskOrImpl, NameBitBoolMaskOr>;

}

REGISTER_FUNCTION(BitBoolMaskOr)
{
    factory.registerFunction<FunctionBitBoolMaskOr>();
}

}
