#pragma once

/** The `taotie` hash - four parallel AES lanes over 512-bit vectors, finished with a MUM fold.
  *
  * Origin: the SMHasher3 copy in `hashes/taotiehash.cpp`. Only the `v2` variant (`TaotieHash64`,
  * built on `hash_long`) is kept; the benchmark registration shell (`REGISTER_FAMILY`,
  * `REGISTER_HASH`, `HashInfo`, `Platform.h`, `Hashlib.h`) and the `v1` variant are dropped.
  *
  * `taotie::hash64` has the same signature as `rapid::rapidhash` and is called from
  * `base/base/RapidStringHash.h` for keys of 256 bytes and above.
  *
  * The whole file is gated on `__AVX512F__ && __VAES__`. Without those it expands to nothing and
  * `rapidStringHash` falls back to `rapidhash` for large keys as well.
  */

#include <cstddef>
#include <cstdint>

#if defined(__AVX512F__) && defined(__VAES__)

#include <immintrin.h>

namespace taotie
{

inline uint64_t mumMix(uint64_t a, uint64_t b)
{
    __uint128_t product = static_cast<__uint128_t>(a) * b;
    return static_cast<uint64_t>(product) ^ static_cast<uint64_t>(product >> 64);
}

/// Load `size` bytes (1..63) into a zero-filled 512-bit vector.
inline __m512i loadMasked(const uint8_t * p, size_t size)
{
    __mmask64 mask = (1ULL << size) - 1;
    return _mm512_maskz_loadu_epi8(mask, p);
}

inline __m512i seed0()
{
    return _mm512_setr_epi32(
        0x396CFEB8, 0xBE4BA423, 0x2C81017C, 0x1CAD21F7,
        0xE96DD4DE, 0xDB979083, 0xA4A44072, 0x1F67B3B7,
        0x4EE679CB, 0x78E5C0CC, 0x7DD05A82, 0x2172FFCC,
        0x744608B8, 0x8E2443F7, 0xE69035E0, 0x4C263A81);
}

inline __m512i seed1()
{
    return _mm512_setr_epi32(
        0xBB52283C, 0xCB00C391, 0x8B65D088, 0xA32E531B,
        0x97486471, 0x4EF90DA2, 0x46EF1938, 0xD8ACDEA9,
        0x3F76FAA8, 0x3F349CE3, 0xC7BBDCF9, 0x1D4F0BC7,
        0x4BE0518A, 0x3159B4CD, 0xC97E9FC8, 0x647378D9);
}

inline __m512i seed2()
{
    return _mm512_setr_epi32(
        0x83ACC5EA, 0xC3EBD334, 0xFFA081C5, 0xEB6313FA,
        0x51DD0D17, 0x49DAF0B7, 0x265516D3, 0x9E68D429,
        0x58BE162B, 0xFCA1477D, 0xD1B8F88F, 0xCE31D07A,
        0x8F3ACB45, 0x28041695, 0xCAFBD7AF, 0x7E404BBB);
}

inline __m512i seed3()
{
    return _mm512_setr_epi64(
        0x2d358dccaa6c78a5ULL,
        0x8bb84b93962eacc9ULL,
        0x4b33a62ed433d4a3ULL,
        0xa76d339a7b1c4e2fULL,
        0xd5a3c8f02e7b9461ULL,
        0x3e6f14b87c2a05d9ULL,
        0xb08247e95f3d6c1aULL,
        0x71c9a0254e8b3f76ULL);
}

inline uint64_t hash64(const void * data, size_t size, uint64_t seed)
{
    const uint8_t * p = static_cast<const uint8_t *>(data);
    size_t remaining = size;

    __m128i seed_128 = _mm_set_epi64x(1 + size + seed, seed);
    __m512i seed_512 = _mm512_broadcast_i32x4(seed_128);

    __m512i hash0;
    __m512i hash1;
    __m512i hash2;
    __m512i hash3;

    if (size >= 256)
    {
        hash0 = _mm512_xor_si512(seed0(), _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p)));
        hash1 = _mm512_xor_si512(seed1(), _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p + 64)));
        hash2 = _mm512_xor_si512(seed2(), _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p + 128)));
        hash3 = _mm512_xor_si512(seed3(), _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p + 192)));
        p += 256;
        remaining -= 256;
    }
    else
    {
        /// Fewer than 256 bytes: xor in whatever each 64-byte lane can see, leaving the rest at the seed value.
        auto xor_lane = [&](__m512i lane_seed, size_t lane_offset) -> __m512i
        {
            if (size <= lane_offset)
                return lane_seed;

            const size_t available = size - lane_offset;
            if (available >= 64)
                return _mm512_xor_si512(lane_seed, _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p + lane_offset)));

            return _mm512_xor_si512(lane_seed, loadMasked(p + lane_offset, available));
        };

        hash0 = xor_lane(seed0(), 0);
        hash1 = xor_lane(seed1(), 64);
        hash2 = xor_lane(seed2(), 128);
        hash3 = xor_lane(seed3(), 192);
        remaining = 0;
    }

    while (remaining >= 256)
    {
        hash0 = _mm512_aesenc_epi128(hash0, _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p)));
        hash1 = _mm512_aesenc_epi128(hash1, _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p + 64)));
        hash2 = _mm512_aesenc_epi128(hash2, _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p + 128)));
        hash3 = _mm512_aesenc_epi128(hash3, _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p + 192)));

        p += 256;
        remaining -= 256;
    }

    if (remaining >= 192)
    {
        hash0 = _mm512_aesenc_epi128(hash0, _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p)));
        hash1 = _mm512_aesenc_epi128(hash1, _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p + 64)));
        hash2 = _mm512_aesenc_epi128(hash2, _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p + 128)));
        hash3 = _mm512_aesenc_epi128(hash3, loadMasked(p + 192, remaining - 192));
    }
    else if (remaining >= 128)
    {
        hash0 = _mm512_aesenc_epi128(hash0, _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p)));
        hash1 = _mm512_aesenc_epi128(hash1, _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p + 64)));
        hash2 = _mm512_aesenc_epi128(hash2, loadMasked(p + 128, remaining - 128));
        hash3 = _mm512_aesenc_epi128(hash3, seed_512);
    }
    else if (remaining >= 64)
    {
        hash0 = _mm512_aesenc_epi128(hash0, _mm512_loadu_si512(reinterpret_cast<const __m512i *>(p)));
        hash1 = _mm512_aesenc_epi128(hash1, loadMasked(p + 64, remaining - 64));
        hash2 = _mm512_aesenc_epi128(hash2, seed_512);
        hash3 = _mm512_aesenc_epi128(hash3, seed_512);
    }
    else if (remaining > 0)
    {
        hash0 = _mm512_aesenc_epi128(hash0, loadMasked(p, remaining));
        hash1 = _mm512_aesenc_epi128(hash1, seed_512);
        hash2 = _mm512_aesenc_epi128(hash2, seed_512);
        hash3 = _mm512_aesenc_epi128(hash3, seed_512);
    }

    for (size_t round = 0; round < 2; ++round)
    {
        hash0 = _mm512_aesenc_epi128(hash0, seed_512);
        hash1 = _mm512_aesenc_epi128(hash1, seed_512);
        hash2 = _mm512_aesenc_epi128(hash2, seed_512);
        hash3 = _mm512_aesenc_epi128(hash3, seed_512);
    }

    hash0 = _mm512_aesenc_epi128(hash0, hash2);
    hash1 = _mm512_aesenc_epi128(hash1, hash3);
    hash0 = _mm512_xor_si512(hash0, hash1);

    __m128i fold = _mm_xor_si128(
        _mm_xor_si128(_mm512_extracti32x4_epi32(hash0, 3), _mm512_extracti32x4_epi32(hash0, 1)),
        _mm_xor_si128(_mm512_extracti32x4_epi32(hash0, 2), _mm512_extracti32x4_epi32(hash0, 0)));

    return mumMix(static_cast<uint64_t>(_mm_cvtsi128_si64(fold)), static_cast<uint64_t>(_mm_extract_epi64(fold, 1)));
}

}

#endif
