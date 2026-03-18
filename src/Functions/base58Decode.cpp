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

using Base58DecodeImpl = Base58Decode<NameBase58Decode, Base58DecodeErrorHandling::ThrowException>;
using FunctionBase58Decode = FunctionBase58Conversion<Base58DecodeImpl>;

}

REGISTER_FUNCTION(Base58Decode)
{
    factory.registerFunction<FunctionBase58Decode>();
}

}
