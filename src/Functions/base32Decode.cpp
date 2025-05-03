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

using Base32DecodeImpl = Base32Decode<NameBase32Decode, Base32DecodeErrorHandling::ThrowException>;
using FunctionBase32Decode = FunctionBase32Conversion<Base32DecodeImpl>;

}

REGISTER_FUNCTION(Base32Decode)
{
    factory.registerFunction<FunctionBase32Decode>();
}

}
