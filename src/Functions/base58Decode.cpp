#include <Functions/FunctionBase58Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase58Decode
{
    static constexpr auto name = "base58Decode";
};

using Base58DecodeImpl = BaseXXDecode<Base58DecodeTraits, NameBase58Decode, BaseXXDecodeErrorHandling::ThrowException>;
using FunctionBase58Decode = FunctionBaseXXConversion<Base58DecodeImpl>;
}

REGISTER_FUNCTION(Base58Decode)
{
    factory.registerFunction<FunctionBase58Decode>();
}

}
