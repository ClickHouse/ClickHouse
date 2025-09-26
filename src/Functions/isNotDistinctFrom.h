#include <Core/AccurateComparison.h>
#include <Functions/FunctionsNullSafeCmp.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct NameFunctionIsNotDistinctFrom { static constexpr auto name = "isNotDistinctFrom"; };

using NullSafeEqualImpl = FunctionsNullSafeCmp<NameFunctionIsNotDistinctFrom,
                                               NullSafeCmpMode::NullSafeEqual,
                                               EqualsOp,
                                               NameEquals>;
using FunctionIsNotDistinctFrom = NullSafeEqualImpl;

}


