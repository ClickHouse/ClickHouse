// metrohash128.cpp
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

#include <string.h>
#include "platform.h"
#include "metrohash128.h"

const char * MetroHash128::test_string = "012345678901234567890123456789012345678901234567890123456789012";

const uint8_t MetroHash128::test_seed_0[16] =   {
                                                0xC7, 0x7C, 0xE2, 0xBF, 0xA4, 0xED, 0x9F, 0x9B,
                                                0x05, 0x48, 0xB2, 0xAC, 0x50, 0x74, 0xA2, 0x97
                                                };

const uint8_t MetroHash128::test_seed_1[16] =   {
                                                0x45, 0xA3, 0xCD, 0xB8, 0x38, 0x19, 0x9D, 0x7F,
                                                0xBD, 0xD6, 0x8D, 0x86, 0x7A, 0x14, 0xEC, 0xEF
                                                };



MetroHash128::MetroHash128(const uint64_t seed)
{
    Initialize(seed);
}


void MetroHash128::Initialize(const uint64_t seed)
{
    // initialize internal hash registers
    state.v[0] = (static_cast<uint64_t>(seed) - k0) * k3;
    state.v[1] = (static_cast<uint64_t>(seed) + k1) * k2;
    state.v[2] = (static_cast<uint64_t>(seed) + k0) * k2;
    state.v[3] = (static_cast<uint64_t>(seed) - k1) * k3;

    // initialize total length of input
    bytes = 0;
}


void MetroHash128::Update(const uint8_t * const buffer, const uint64_t length)
{
    const uint8_t * ptr = reinterpret_cast<const uint8_t*>(buffer);
    const uint8_t * const end = ptr + length;

    // input buffer may be partially filled
    if (bytes % 32)
    {
        uint64_t fill = 32 - (bytes % 32);
        if (fill > length)
            fill = length;

        memcpy(input.b + (bytes % 32), ptr, static_cast<size_t>(fill));
        ptr   += fill;
        bytes += fill;
        
        // input buffer is still partially filled
        if ((bytes % 32) != 0) return;

        // process full input buffer
        state.v[0] += read_u64(&input.b[ 0]) * k0; state.v[0] = rotate_right(state.v[0],29) + state.v[2];
        state.v[1] += read_u64(&input.b[ 8]) * k1; state.v[1] = rotate_right(state.v[1],29) + state.v[3];
        state.v[2] += read_u64(&input.b[16]) * k2; state.v[2] = rotate_right(state.v[2],29) + state.v[0];
        state.v[3] += read_u64(&input.b[24]) * k3; state.v[3] = rotate_right(state.v[3],29) + state.v[1];
    }
    
    // bulk update
    bytes += (end - ptr);
    while (ptr <= (end - 32))
    {
        // process directly from the source, bypassing the input buffer
        state.v[0] += read_u64(ptr) * k0; ptr += 8; state.v[0] = rotate_right(state.v[0],29) + state.v[2];
        state.v[1] += read_u64(ptr) * k1; ptr += 8; state.v[1] = rotate_right(state.v[1],29) + state.v[3];
        state.v[2] += read_u64(ptr) * k2; ptr += 8; state.v[2] = rotate_right(state.v[2],29) + state.v[0];
        state.v[3] += read_u64(ptr) * k3; ptr += 8; state.v[3] = rotate_right(state.v[3],29) + state.v[1];
    }
    
    // store remaining bytes in input buffer
    if (ptr < end)
        memcpy(input.b, ptr, end - ptr);
}


void MetroHash128::Finalize(uint8_t * const hash)
{
    // finalize bulk loop, if used
    if (bytes >= 32)
    {
        state.v[2] ^= rotate_right(((state.v[0] + state.v[3]) * k0) + state.v[1], 21) * k1;
        state.v[3] ^= rotate_right(((state.v[1] + state.v[2]) * k1) + state.v[0], 21) * k0;
        state.v[0] ^= rotate_right(((state.v[0] + state.v[2]) * k0) + state.v[3], 21) * k1;
        state.v[1] ^= rotate_right(((state.v[1] + state.v[3]) * k1) + state.v[2], 21) * k0;
    }
    
    // process any bytes remaining in the input buffer
    const uint8_t * ptr = reinterpret_cast<const uint8_t*>(input.b);
    const uint8_t * const end = ptr + (bytes % 32);
    
    if ((end - ptr) >= 16)
    {
        state.v[0] += read_u64(ptr) * k2; ptr += 8; state.v[0] = rotate_right(state.v[0],33) * k3;
        state.v[1] += read_u64(ptr) * k2; ptr += 8; state.v[1] = rotate_right(state.v[1],33) * k3;
        state.v[0] ^= rotate_right((state.v[0] * k2) + state.v[1], 45) * k1;
        state.v[1] ^= rotate_right((state.v[1] * k3) + state.v[0], 45) * k0;
    }

    if ((end - ptr) >= 8)
    {
        state.v[0] += read_u64(ptr) * k2; ptr += 8; state.v[0] = rotate_right(state.v[0],33) * k3;
        state.v[0] ^= rotate_right((state.v[0] * k2) + state.v[1], 27) * k1;
    }

    if ((end - ptr) >= 4)
    {
        state.v[1] += read_u32(ptr) * k2; ptr += 4; state.v[1] = rotate_right(state.v[1],33) * k3;
        state.v[1] ^= rotate_right((state.v[1] * k3) + state.v[0], 46) * k0;
    }

    if ((end - ptr) >= 2)
    {
        state.v[0] += read_u16(ptr) * k2; ptr += 2; state.v[0] = rotate_right(state.v[0],33) * k3;
        state.v[0] ^= rotate_right((state.v[0] * k2) + state.v[1], 22) * k1;
    }

    if ((end - ptr) >= 1)
    {
        state.v[1] += read_u8 (ptr) * k2; state.v[1] = rotate_right(state.v[1],33) * k3;
        state.v[1] ^= rotate_right((state.v[1] * k3) + state.v[0], 58) * k0;
    }
    
    state.v[0] += rotate_right((state.v[0] * k0) + state.v[1], 13);
    state.v[1] += rotate_right((state.v[1] * k1) + state.v[0], 37);
    state.v[0] += rotate_right((state.v[0] * k2) + state.v[1], 13);
    state.v[1] += rotate_right((state.v[1] * k3) + state.v[0], 37);

    bytes = 0;

    // do any endian conversion here

    memcpy(hash, state.v, 16);
}


void MetroHash128::Hash(const uint8_t * buffer, const uint64_t length, uint8_t * const hash, const uint64_t seed)
{
    const uint8_t * ptr = reinterpret_cast<const uint8_t*>(buffer);
    const uint8_t * const end = ptr + length;

    uint64_t v[4];

    v[0] = (static_cast<uint64_t>(seed) - k0) * k3;
    v[1] = (static_cast<uint64_t>(seed) + k1) * k2;

    if (length >= 32)
    {
        v[2] = (static_cast<uint64_t>(seed) + k0) * k2;
        v[3] = (static_cast<uint64_t>(seed) - k1) * k3;

        do
        {
            v[0] += read_u64(ptr) * k0; ptr += 8; v[0] = rotate_right(v[0],29) + v[2];
            v[1] += read_u64(ptr) * k1; ptr += 8; v[1] = rotate_right(v[1],29) + v[3];
            v[2] += read_u64(ptr) * k2; ptr += 8; v[2] = rotate_right(v[2],29) + v[0];
            v[3] += read_u64(ptr) * k3; ptr += 8; v[3] = rotate_right(v[3],29) + v[1];
        }
        while (ptr <= (end - 32));

        v[2] ^= rotate_right(((v[0] + v[3]) * k0) + v[1], 21) * k1;
        v[3] ^= rotate_right(((v[1] + v[2]) * k1) + v[0], 21) * k0;
        v[0] ^= rotate_right(((v[0] + v[2]) * k0) + v[3], 21) * k1;
        v[1] ^= rotate_right(((v[1] + v[3]) * k1) + v[2], 21) * k0;
    }

    if ((end - ptr) >= 16)
    {
        v[0] += read_u64(ptr) * k2; ptr += 8; v[0] = rotate_right(v[0],33) * k3;
        v[1] += read_u64(ptr) * k2; ptr += 8; v[1] = rotate_right(v[1],33) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 45) * k1;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 45) * k0;
    }

    if ((end - ptr) >= 8)
    {
        v[0] += read_u64(ptr) * k2; ptr += 8; v[0] = rotate_right(v[0],33) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 27) * k1;
    }

    if ((end - ptr) >= 4)
    {
        v[1] += read_u32(ptr) * k2; ptr += 4; v[1] = rotate_right(v[1],33) * k3;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 46) * k0;
    }

    if ((end - ptr) >= 2)
    {
        v[0] += read_u16(ptr) * k2; ptr += 2; v[0] = rotate_right(v[0],33) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 22) * k1;
    }

    if ((end - ptr) >= 1)
    {
        v[1] += read_u8 (ptr) * k2; v[1] = rotate_right(v[1],33) * k3;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 58) * k0;
    }

    v[0] += rotate_right((v[0] * k0) + v[1], 13);
    v[1] += rotate_right((v[1] * k1) + v[0], 37);
    v[0] += rotate_right((v[0] * k2) + v[1], 13);
    v[1] += rotate_right((v[1] * k3) + v[0], 37);

    // do any endian conversion here

    memcpy(hash, v, 16);
}


bool MetroHash128::ImplementationVerified()
{
    uint8_t hash[16];
    const uint8_t * key = reinterpret_cast<const uint8_t *>(MetroHash128::test_string);

    // verify one-shot implementation
    MetroHash128::Hash(key, strlen(MetroHash128::test_string), hash, 0);
    if (memcmp(hash, MetroHash128::test_seed_0, 16) != 0) return false;

    MetroHash128::Hash(key, strlen(MetroHash128::test_string), hash, 1);
    if (memcmp(hash, MetroHash128::test_seed_1, 16) != 0) return false;

    // verify incremental implementation
    MetroHash128 metro;
    
    metro.Initialize(0);
    metro.Update(reinterpret_cast<const uint8_t *>(MetroHash128::test_string), strlen(MetroHash128::test_string));
    metro.Finalize(hash);
    if (memcmp(hash, MetroHash128::test_seed_0, 16) != 0) return false;

    metro.Initialize(1);
    metro.Update(reinterpret_cast<const uint8_t *>(MetroHash128::test_string), strlen(MetroHash128::test_string));
    metro.Finalize(hash);
    if (memcmp(hash, MetroHash128::test_seed_1, 16) != 0) return false;

    return true;
}


void metrohash128_1(const uint8_t * key, uint64_t len, uint32_t seed, uint8_t * out)
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
            v[0] += read_u64(ptr) * k0; ptr += 8; v[0] = rotate_right(v[0],29) + v[2];
            v[1] += read_u64(ptr) * k1; ptr += 8; v[1] = rotate_right(v[1],29) + v[3];
            v[2] += read_u64(ptr) * k2; ptr += 8; v[2] = rotate_right(v[2],29) + v[0];
            v[3] += read_u64(ptr) * k3; ptr += 8; v[3] = rotate_right(v[3],29) + v[1];
        }
        while (ptr <= (end - 32));

        v[2] ^= rotate_right(((v[0] + v[3]) * k0) + v[1], 26) * k1;
        v[3] ^= rotate_right(((v[1] + v[2]) * k1) + v[0], 26) * k0;
        v[0] ^= rotate_right(((v[0] + v[2]) * k0) + v[3], 26) * k1;
        v[1] ^= rotate_right(((v[1] + v[3]) * k1) + v[2], 30) * k0;
    }
    
    if ((end - ptr) >= 16)
    {
        v[0] += read_u64(ptr) * k2; ptr += 8; v[0] = rotate_right(v[0],33) * k3;
        v[1] += read_u64(ptr) * k2; ptr += 8; v[1] = rotate_right(v[1],33) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 17) * k1;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 17) * k0;
    }
    
    if ((end - ptr) >= 8)
    {
        v[0] += read_u64(ptr) * k2; ptr += 8; v[0] = rotate_right(v[0],33) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 20) * k1;
    }
    
    if ((end - ptr) >= 4)
    {
        v[1] += read_u32(ptr) * k2; ptr += 4; v[1] = rotate_right(v[1],33) * k3;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 18) * k0;
    }
    
    if ((end - ptr) >= 2)
    {
        v[0] += read_u16(ptr) * k2; ptr += 2; v[0] = rotate_right(v[0],33) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 24) * k1;
    }
    
    if ((end - ptr) >= 1)
    {
        v[1] += read_u8 (ptr) * k2; v[1] = rotate_right(v[1],33) * k3;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 24) * k0;
    }
    
    v[0] += rotate_right((v[0] * k0) + v[1], 13);
    v[1] += rotate_right((v[1] * k1) + v[0], 37);
    v[0] += rotate_right((v[0] * k2) + v[1], 13);
    v[1] += rotate_right((v[1] * k3) + v[0], 37);

    // do any endian conversion here

    memcpy(out, v, 16);
}


void metrohash128_2(const uint8_t * key, uint64_t len, uint32_t seed, uint8_t * out)
{
    static const uint64_t k0 = 0xD6D018F5;
    static const uint64_t k1 = 0xA2AA033B;
    static const uint64_t k2 = 0x62992FC1;
    static const uint64_t k3 = 0x30BC5B29; 

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
            v[0] += read_u64(ptr) * k0; ptr += 8; v[0] = rotate_right(v[0],29) + v[2];
            v[1] += read_u64(ptr) * k1; ptr += 8; v[1] = rotate_right(v[1],29) + v[3];
            v[2] += read_u64(ptr) * k2; ptr += 8; v[2] = rotate_right(v[2],29) + v[0];
            v[3] += read_u64(ptr) * k3; ptr += 8; v[3] = rotate_right(v[3],29) + v[1];
        }
        while (ptr <= (end - 32));

        v[2] ^= rotate_right(((v[0] + v[3]) * k0) + v[1], 33) * k1;
        v[3] ^= rotate_right(((v[1] + v[2]) * k1) + v[0], 33) * k0;
        v[0] ^= rotate_right(((v[0] + v[2]) * k0) + v[3], 33) * k1;
        v[1] ^= rotate_right(((v[1] + v[3]) * k1) + v[2], 33) * k0;
    }
    
    if ((end - ptr) >= 16)
    {
        v[0] += read_u64(ptr) * k2; ptr += 8; v[0] = rotate_right(v[0],29) * k3;
        v[1] += read_u64(ptr) * k2; ptr += 8; v[1] = rotate_right(v[1],29) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 29) * k1;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 29) * k0;
    }
    
    if ((end - ptr) >= 8)
    {
        v[0] += read_u64(ptr) * k2; ptr += 8; v[0] = rotate_right(v[0],29) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 29) * k1;
    }
    
    if ((end - ptr) >= 4)
    {
        v[1] += read_u32(ptr) * k2; ptr += 4; v[1] = rotate_right(v[1],29) * k3;
        v[1] ^= rotate_right((v[1] * k3) + v[0], 25) * k0;
    }
    
    if ((end - ptr) >= 2)
    {
        v[0] += read_u16(ptr) * k2; ptr += 2; v[0] = rotate_right(v[0],29) * k3;
        v[0] ^= rotate_right((v[0] * k2) + v[1], 30) * k1;
    }
    
    if ((end - ptr) >= 1)
    {
          v[1] += read_u8 (ptr) * k2; v[1] = rotate_right(v[1],29) * k3;
          v[1] ^= rotate_right((v[1] * k3) + v[0], 18) * k0;
    }
    
    v[0] += rotate_right((v[0] * k0) + v[1], 33);
    v[1] += rotate_right((v[1] * k1) + v[0], 33);
    v[0] += rotate_right((v[0] * k2) + v[1], 33);
    v[1] += rotate_right((v[1] * k3) + v[0], 33);

    // do any endian conversion here

    memcpy(out, v, 16);
}

