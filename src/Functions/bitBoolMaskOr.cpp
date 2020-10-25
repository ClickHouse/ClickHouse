#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <DataTypes/NumberTraits.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
    }

    /// Working with UInt8: last bit = can be true, previous = can be false (Like src/Storages/MergeTree/BoolMask.h).
    /// This function provides "OR" operation for BoolMasks.
    /// Returns: "can be true" = A."can be true" OR B."can be true"
    ///          "can be false" = A."can be false" AND B."can be false"
    template <typename A, typename B>
    struct BitBoolMaskOrImpl
    {
        using ResultType = UInt8;
        static const constexpr bool allow_fixed_string = false;

        template <typename Result = ResultType>
        static inline Result apply(A left, B right)
        {
            if constexpr (!std::is_same_v<A, ResultType> || !std::is_same_v<B, ResultType>)
                // Should be a logical error, but this function is callable from SQL.
                // Need to investigate this.
                throw DB::Exception("It's a bug! Only UInt8 type is supported by __bitBoolMaskOr.", ErrorCodes::BAD_ARGUMENTS);
            return static_cast<ResultType>(
                    ((static_cast<ResultType>(left) | static_cast<ResultType>(right)) & 1)
                    | ((((static_cast<ResultType>(left) >> 1) & (static_cast<ResultType>(right) >> 1)) & 1) << 1));
        }

#if USE_EMBEDDED_COMPILER
        static constexpr bool compilable = false;

#endif
    };

    struct NameBitBoolMaskOr { static constexpr auto name = "__bitBoolMaskOr"; };
    using FunctionBitBoolMaskOr = FunctionBinaryArithmetic<BitBoolMaskOrImpl, NameBitBoolMaskOr>;

    void registerFunctionBitBoolMaskOr(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionBitBoolMaskOr>();
    }

}
