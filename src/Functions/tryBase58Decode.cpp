#include <Functions/FunctionBase58Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameTryBase58Decode
{
    static constexpr auto name = "tryBase58Decode";
};

using TryBase58DecodeImpl = BaseXXDecode<Base58DecodeTraits, NameTryBase58Decode, BaseXXDecodeErrorHandling::ReturnEmptyString>;
using FunctionTryBase58Decode = FunctionBaseXXConversion<TryBase58DecodeImpl>;
}

REGISTER_FUNCTION(TryBase58Decode)
{
    factory.registerFunction<FunctionTryBase58Decode>();
}

}
