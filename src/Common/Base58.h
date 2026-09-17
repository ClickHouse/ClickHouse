#pragma once

#include <base/types.h>
#include <optional>


namespace DB
{

size_t encodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst);
std::optional<size_t> decodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst);

}
