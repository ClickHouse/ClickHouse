#include <Functions/FunctionBase58Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase58Encode
{
    static constexpr auto name = "base58Encode";
};

using Base58EncodeImpl = BaseXXEncode<Base58EncodeTraits, NameBase58Encode>;
using FunctionBase58Encode = FunctionBaseXXConversion<Base58EncodeImpl>;
}
REGISTER_FUNCTION(Base58Encode)
{
    factory.registerFunction<FunctionBase58Encode>();
}

}
