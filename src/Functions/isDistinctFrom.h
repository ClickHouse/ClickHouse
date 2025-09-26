#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsNullSafeCmp.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct NameFunctionIsDistinctFrom { static constexpr auto name = "isDistinctFrom"; };

using NullSafeNotEqualImpl = DB::FunctionsNullSafeCmp<NameFunctionIsDistinctFrom,
                                                      NullSafeCmpMode::NullSafeNotEqual,
                                                      NotEqualsOp,
                                                      NameNotEquals>;
using FunctionIsDistinctFrom = NullSafeNotEqualImpl;

}


