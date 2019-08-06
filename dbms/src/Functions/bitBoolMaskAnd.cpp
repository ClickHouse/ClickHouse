#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{

    /// Working with UInt8: last bit = can be true, previous = can be false (Like dbms/src/Storages/MergeTree/BoolMask.h).
    /// This function provides "AND" operation for BoolMasks.
    /// Returns: "can be true" = A."can be true" AND B."can be true"
    ///          "can be false" = A."can be false" OR B."can be false"
    template <typename A, typename B>
    struct BitBoolMaskAndImpl
    {
        using ResultType = UInt8;

        template <typename Result = ResultType>
        static inline Result apply(A left, B right)
        {
            return static_cast<ResultType>(
                    ((static_cast<ResultType>(left) & static_cast<ResultType>(right)) & 1)
                    | ((((static_cast<ResultType>(left) >> 1) | (static_cast<ResultType>(right) >> 1)) & 1) << 1));
        }

#if USE_EMBEDDED_COMPILER
        static constexpr bool compilable = false;

#endif
    };

    struct NameBitBoolMaskAnd { static constexpr auto name = "__bitBoolMaskAnd"; };
    using FunctionBitBoolMaskAnd = FunctionBinaryArithmetic<BitBoolMaskAndImpl, NameBitBoolMaskAnd>;

    void registerFunctionBitBoolMaskAnd(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionBitBoolMaskAnd>();
    }

}
