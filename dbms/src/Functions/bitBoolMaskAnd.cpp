#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{

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
