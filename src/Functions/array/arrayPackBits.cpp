#include <Functions/array/FunctionArrayMapped.h>

namespace DB
{
// implementation arrayPackBitsToUInt64 first
struct ArrayPackBitsToUint64Impl
{
};
struct NameArrayPackBitsToUInt64
{
    static constexpr auto name = "arrayPackBitsToUInt64";
};

using FunctionArrayMin = FunctionArrayMapped<ArrayPackBitsToUint64Impl, NameArrayPackBitsToUInt64>;
}
