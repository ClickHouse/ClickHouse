// platform.h
//
// Copyright 2015-2018 J. Andrew Rogers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef METROHASH_PLATFORM_H
#define METROHASH_PLATFORM_H

#include <stdint.h>
#include <cstring>

// rotate right idiom recognized by most compilers
inline static uint64_t rotate_right(uint64_t v, unsigned k)
{
    return (v >> k) | (v << (64 - k));
}

inline static uint64_t read_u64(const void * const ptr)
{
    uint64_t result;
    // Assignment like `result = *reinterpret_cast<const uint64_t *>(ptr)` here would mean undefined behaviour (unaligned read),
    // so we use memcpy() which is the most portable. clang & gcc usually translates `memcpy()` into a single `load` instruction
    // when hardware supports it, so using memcpy() is efficient too.
    memcpy(&result, ptr, sizeof(result));
    return result;
}

inline static uint64_t read_u32(const void * const ptr)
{
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
}

inline static uint64_t read_u16(const void * const ptr)
{
    uint16_t result;
    memcpy(&result, ptr, sizeof(result));
    return result;
}

inline static uint64_t read_u8 (const void * const ptr)
{
    return static_cast<uint64_t>(*reinterpret_cast<const uint8_t *>(ptr));
}


#endif // #ifndef METROHASH_PLATFORM_H
