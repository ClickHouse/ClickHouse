#pragma once

#include <base/find_symbols.h>
#include <base/hex.h>


size_t encodeURL(const char * __restrict src, size_t src_size, char * __restrict dst, bool space_as_plus);
/// We assume that size of the dst buf isn't less than src_size.
size_t decodeURL(const char * __restrict src, size_t src_size, char * __restrict dst, bool plus_as_space);
