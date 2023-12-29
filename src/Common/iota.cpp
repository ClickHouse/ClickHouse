#include <Common/iota.h>

namespace DB
{
template void iota(UInt64 * begin, size_t count, UInt64 first_value);
}
