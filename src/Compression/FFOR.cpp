#include <Compression/FFOR.h>

namespace DB::Compression::FFOR
{
template void bitPack<DEFAULT_VALUES>(const UInt16 * __restrict, UInt16 * __restrict, UInt8, UInt16);
template void bitPack<DEFAULT_VALUES>(const UInt32 * __restrict, UInt32 * __restrict, UInt8, UInt32);
template void bitPack<DEFAULT_VALUES>(const UInt64 * __restrict, UInt64 * __restrict, UInt8, UInt64);

template void bitUnpack<DEFAULT_VALUES>(const UInt16 * __restrict, UInt16 * __restrict, UInt8, UInt16);
template void bitUnpack<DEFAULT_VALUES>(const UInt32 * __restrict, UInt32 * __restrict, UInt8, UInt32);
template void bitUnpack<DEFAULT_VALUES>(const UInt64 * __restrict, UInt64 * __restrict, UInt8, UInt64);
}
