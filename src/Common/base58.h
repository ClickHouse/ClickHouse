#pragma once

#include <Core/Types.h>


namespace DB
{

size_t encodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst);
size_t decodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst);

}
