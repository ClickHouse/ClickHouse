#pragma once

#include <base/types.h>
#include <optional>


namespace DB
{

void encodeBase32(const UInt8 * src, size_t src_length, UInt8 * dst);
std::optional<size_t> decodeBase32(const UInt8 * src, size_t src_length, UInt8 * dst);

}
