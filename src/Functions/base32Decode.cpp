#include <Functions/FunctionBase32Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct NameBase32Decode
{
    static constexpr auto name = "base32Decode";
};


using Base32DecodeImpl = BaseXXDecode<Base32Traits, NameBase32Decode, BaseXXDecodeErrorHandling::ThrowException>;
using FunctionBase32Decode = FunctionBaseXXConversion<Base32DecodeImpl>;

}

REGISTER_FUNCTION(Base32Decode)
{
    factory.registerFunction<FunctionBase32Decode>();
}
}
