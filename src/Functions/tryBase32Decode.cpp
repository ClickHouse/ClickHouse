#include <Functions/FunctionBase32Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct NameTryBase32Decode
{
    static constexpr auto name = "tryBase32Decode";
};

using TryBase32DecodeImpl = BaseXXDecode<Base32Traits, NameTryBase32Decode, BaseXXDecodeErrorHandling::ReturnEmptyString>;
using FunctionTryBase32Decode = FunctionBaseXXConversion<TryBase32DecodeImpl>;

}

REGISTER_FUNCTION(TryBase32Decode)
{
    factory.registerFunction<FunctionTryBase32Decode>();
}

}
