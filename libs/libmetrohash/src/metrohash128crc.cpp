// metrohash128crc.cpp
//
// The MIT License (MIT)
//
// Copyright (c) 2015 J. Andrew Rogers
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//


#include "metrohash.h"
#include <nmmintrin.h>


void metrohash128crc_1(const uint8_t * key, uint64_t len, uint32_t seed, uint8_t * out)
{
    static const uint64_t k0 = 0xC83A91E1;
    static const uint64_t k1 = 0x8648DBDB;
    static const uint64_t k2 = 0x7BDEC03B;
    static const uint64_t k3 = 0x2F5870A5;

    const uint8_t * ptr = reinterpret_cast<const uint8_t*>(key);
    const uint8_t * const end = ptr + len;
    
    uint64_t v[4];
    
    v[0] = ((static_cast<uint64_t>(seed) - k0) * k3) + len;
    v[1] = ((static_cast<uint64_t>(seed) + k1) * k2) + len;
    
    if (len >= 32)
    {        
        v[2] = ((static_cast<uint64_t>(seed) + k0) * k2) + len;
        v[3] = ((static_cast<uint64_t>(seed) - k1) * k3) + len;

        do
        {
            v[0] ^= _mm_crc32_u64(v[0], read_u64(ptr)); ptr += 8;
            v[1] ^= _mm_crc32_u64(v[1], read_u64(ptr)); ptr += 8;
            v[2] ^= _mm_crc32_u64(v[2], read_u64(ptr)); ptr += 8;
            v[3] ^= _mm_crc32_u64(v[3], read_u64(ptr)); ptr += 8;
        }
        while (ptr <= (end - 32));

        v[2] ^= rotate_right(((v[0] + v[3]) * k0) + v[1], 34) * k1;
        v[3] ^= rotate_right(((v[1] + v[2]) * k1) + v[0], 37) * k0;
        v[0] ^= rotate_right(((v[0] + v[2]) * k0) + v[3], 34) * k1;
        v[1] ^= rotate_right(((v[1] + v[3]) * k1) + v[2], 37) * k0;
    }
    
    if ((end - ptr) >= 16)
    {
        v[0] += read_u64(ptr) * k2; ptr += 8; v[0] = rotate_right(v[0],34) * k3;
        v[1] += read_u64(ptr) * k2; ptr += 8; v[1] = rotate_right(v[1],34) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 30) * k1;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 30) * k0;
    }
    
    if ((end - ptr) >= 8)
    {
        v[0] += read_u64(ptr) * k2; ptr += 8; v[0] = rotate_right(v[0],36) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 23) * k1;
    }
    
    if ((end - ptr) >= 4)
    {
        v[1] ^= _mm_crc32_u64(v[0], read_u32(ptr)); ptr += 4;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 19) * k0;
    }
    
    if ((end - ptr) >= 2)
    {
        v[0] ^= _mm_crc32_u64(v[1], read_u16(ptr)); ptr += 2;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 13) * k1;
    }
    
    if ((end - ptr) >= 1)
    {
        v[1] ^= _mm_crc32_u64(v[0], read_u8 (ptr));
        v[1] ^= rotate_right((v[1] * k3) + v[0], 17) * k0;
    }
    
    v[0] += rotate_right((v[0] * k0) + v[1], 11);
    v[1] += rotate_right((v[1] * k1) + v[0], 26);
    v[0] += rotate_right((v[0] * k0) + v[1], 11);
    v[1] += rotate_right((v[1] * k1) + v[0], 26);

    memcpy(out, v, 16);
}


void metrohash128crc_2(const uint8_t * key, uint64_t len, uint32_t seed, uint8_t * out)
{
    static const uint64_t k0 = 0xEE783E2F;
    static const uint64_t k1 = 0xAD07C493;
    static const uint64_t k2 = 0x797A90BB;
    static const uint64_t k3 = 0x2E4B2E1B;

    const uint8_t * ptr = reinterpret_cast<const uint8_t*>(key);
    const uint8_t * const end = ptr + len;
    
    uint64_t v[4];
    
    v[0] = ((static_cast<uint64_t>(seed) - k0) * k3) + len;
    v[1] = ((static_cast<uint64_t>(seed) + k1) * k2) + len;
    
    if (len >= 32)
    {        
        v[2] = ((static_cast<uint64_t>(seed) + k0) * k2) + len;
        v[3] = ((static_cast<uint64_t>(seed) - k1) * k3) + len;

        do
        {
            v[0] ^= _mm_crc32_u64(v[0], read_u64(ptr)); ptr += 8;
            v[1] ^= _mm_crc32_u64(v[1], read_u64(ptr)); ptr += 8;
            v[2] ^= _mm_crc32_u64(v[2], read_u64(ptr)); ptr += 8;
            v[3] ^= _mm_crc32_u64(v[3], read_u64(ptr)); ptr += 8;
        }
        while (ptr <= (end - 32));

        v[2] ^= rotate_right(((v[0] + v[3]) * k0) + v[1], 12) * k1;
        v[3] ^= rotate_right(((v[1] + v[2]) * k1) + v[0], 19) * k0;
        v[0] ^= rotate_right(((v[0] + v[2]) * k0) + v[3], 12) * k1;
        v[1] ^= rotate_right(((v[1] + v[3]) * k1) + v[2], 19) * k0;
    }
    
    if ((end - ptr) >= 16)
    {
        v[0] += read_u64(ptr) * k2; ptr += 8; v[0] = rotate_right(v[0],41) * k3;
        v[1] += read_u64(ptr) * k2; ptr += 8; v[1] = rotate_right(v[1],41) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 10) * k1;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 10) * k0;
    }
    
    if ((end - ptr) >= 8)
    {
        v[0] += read_u64(ptr) * k2; ptr += 8; v[0] = rotate_right(v[0],34) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 22) * k1;
    }
    
    if ((end - ptr) >= 4)
    {
        v[1] ^= _mm_crc32_u64(v[0], read_u32(ptr)); ptr += 4;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 14) * k0;
    }
    
    if ((end - ptr) >= 2)
    {
        v[0] ^= _mm_crc32_u64(v[1], read_u16(ptr)); ptr += 2;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 15) * k1;
    }
    
    if ((end - ptr) >= 1)
    {
        v[1] ^= _mm_crc32_u64(v[0], read_u8 (ptr));
        v[1] ^= rotate_right((v[1] * k3) + v[0],  18) * k0;
    }
    
    v[0] += rotate_right((v[0] * k0) + v[1], 15);
    v[1] += rotate_right((v[1] * k1) + v[0], 27);
    v[0] += rotate_right((v[0] * k0) + v[1], 15);
    v[1] += rotate_right((v[1] * k1) + v[0], 27);

    memcpy(out, v, 16);
}
