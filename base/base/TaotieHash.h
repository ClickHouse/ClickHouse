#pragma once
// TaotieHash.h — taotie 哈希的纯算法实现（AVX-512 + VAES，4 路 AES + MUM）。
//
// 从 smhasher-main 的 taotiehash.cpp 抽取：去掉了 SMHasher benchmark 的注册外壳
// （REGISTER_FAMILY / REGISTER_HASH / HashInfo / Platform.h / Hashlib.h）和 v1 变体
// （hash_long_v1 / TaotieHash64_v1），只保留 v2（TaotieHash64 -> hash_long）这一支。
//
// 对外入口 taotie::hash64 的签名对齐 rapid::rapidhash：
//     uint64_t(const void * data, size_t len, uint64_t seed)
// 由 <base/RapidStringHash.h> 在 key.size() >= 256 时调用。
//
// 整个文件由 __AVX512F__ && __VAES__ 门控；无 AVX-512 的平台上本头文件展开为空，
// RapidStringHash.h 的 #else 分支会回退到 rapidhash。

#include <cstddef>
#include <cstdint>

#if defined(__AVX512F__) && defined(__VAES__)

#include <immintrin.h>

namespace taotie
{

static inline uint64_t mum_mix(uint64_t a, uint64_t b) {
    __uint128_t p = (__uint128_t)a * b;
    return (uint64_t)p ^ (uint64_t)(p >> 64);
}

static inline __m512i load_64bytes_masked(const uint8_t* p, size_t len) {
    __mmask64 mask = (1ULL << len) - 1;  // len ∈ [1,63]
    return _mm512_maskz_loadu_epi8(mask, p);
}

// Load up to 64 bytes; nb==0 yields a zero block (matches old maskz on exact 64*k tails).
static inline __m512i load_lane64(const uint8_t* p, size_t nb) {
    if (nb >= 64) {
        return _mm512_loadu_si512(reinterpret_cast<const __m512i*>(p));
    }
    if (nb > 0) {
        return load_64bytes_masked(p, nb);
    }
    return _mm512_setzero_si512();
}

static inline __m512i seed512_0() {
    return _mm512_setr_epi32(
        0x396CFEB8u, 0xBE4BA423u, 0x2C81017Cu, 0x1CAD21F7u,
        0xE96DD4DEu, 0xDB979083u, 0xA4A44072u, 0x1F67B3B7u,
        0x4EE679CBu, 0x78E5C0CCu, 0x7DD05A82u, 0x2172FFCCu,
        0x744608B8u, 0x8E2443F7u, 0xE69035E0u, 0x4C263A81u
    );
}

static inline __m512i seed512_1() {
    return _mm512_setr_epi32(
        0xBB52283Cu, 0xCB00C391u, 0x8B65D088u, 0xA32E531Bu,
        0x97486471u, 0x4EF90DA2u, 0x46EF1938u, 0xD8ACDEA9u,
        0x3F76FAA8u, 0x3F349CE3u, 0xC7BBDCF9u, 0x1D4F0BC7u,
        0x4BE0518Au, 0x3159B4CDu, 0xC97E9FC8u, 0x647378D9u
    );
}

static inline __m512i seed512_2() {
    return _mm512_setr_epi32(
        0x83ACC5EAu, 0xC3EBD334u, 0xFFA081C5u, 0xEB6313FAu,
        0x51DD0D17u, 0x49DAF0B7u, 0x265516D3u, 0x9E68D429u,
        0x58BE162Bu, 0xFCA1477Du, 0xD1B8F88Fu, 0xCE31D07Au,
        0x8F3ACB45u, 0x28041695u, 0xCAFBD7AFu, 0x7E404BBBu
    );
}

static inline __m512i seed512_3() {
    return _mm512_setr_epi64(
        UINT64_C(0x2d358dccaa6c78a5),
        UINT64_C(0x8bb84b93962eacc9),
        UINT64_C(0x4b33a62ed433d4a3),
        UINT64_C(0xa76d339a7b1c4e2f),
        UINT64_C(0xd5a3c8f02e7b9461),
        UINT64_C(0x3e6f14b87c2a05d9),
        UINT64_C(0xb08247e95f3d6c1a),
        UINT64_C(0x71c9a0254e8b3f76)
    );
}

static inline uint64_t hash_long(const void* in, size_t len, uint64_t seed) {
    const uint8_t* buf = static_cast<const uint8_t*>(in);
    size_t remaining = len;

    __m128i seed128 = _mm_set_epi64x(1ULL + len + seed, seed);
    __m512i seed512 = _mm512_broadcast_i32x4(seed128);

    __m512i hash0, hash1, hash2, hash3;

    if (len >= 256) {
        hash0 = _mm512_xor_si512(seed512_0(), _mm512_loadu_si512(reinterpret_cast<const __m512i*>(buf + 0)));
        hash1 = _mm512_xor_si512(seed512_1(), _mm512_loadu_si512(reinterpret_cast<const __m512i*>(buf + 64)));
        hash2 = _mm512_xor_si512(seed512_2(), _mm512_loadu_si512(reinterpret_cast<const __m512i*>(buf + 128)));
        hash3 = _mm512_xor_si512(seed512_3(), _mm512_loadu_si512(reinterpret_cast<const __m512i*>(buf + 192)));
        buf += 256;
        remaining -= 256;
    } else {
        auto lane_bytes = [len](size_t lane_off) -> size_t {
            if (len <= lane_off) {
                return 0;
            }
            const size_t avail = len - lane_off;
            return avail >= 64 ? 64 : avail;
        };
        auto xor_lane = [&](__m512i ci, size_t lane_off) -> __m512i {
            const size_t nb = lane_bytes(lane_off);
            if (nb == 0) {
                return ci;
            }
            if (nb == 64) {
                return _mm512_xor_si512(
                    ci, _mm512_loadu_si512(reinterpret_cast<const __m512i*>(buf + lane_off)));
            }
            return _mm512_xor_si512(ci, load_64bytes_masked(buf + lane_off, nb));
        };
        hash0 = xor_lane(seed512_0(), 0);
        hash1 = xor_lane(seed512_1(), 64);
        hash2 = xor_lane(seed512_2(), 128);
        hash3 = xor_lane(seed512_3(), 192);
        remaining = 0;
    }

    while (remaining >= 256) {
        __m512i data0 = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(buf + 0));
        __m512i data1 = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(buf + 64));
        __m512i data2 = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(buf + 128));
        __m512i data3 = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(buf + 192));

        hash0 = _mm512_aesenc_epi128(hash0, data0);
        hash1 = _mm512_aesenc_epi128(hash1, data1);
        hash2 = _mm512_aesenc_epi128(hash2, data2);
        hash3 = _mm512_aesenc_epi128(hash3, data3);

        buf += 256;
        remaining -= 256;
    }

    if (remaining >= 192) {
        __m512i data0 = _mm512_loadu_si512((const __m512i*)(buf + 0));
        __m512i data1 = _mm512_loadu_si512((const __m512i*)(buf + 64));
        __m512i data2 = _mm512_loadu_si512((const __m512i*)(buf + 128));
        __m512i data3 = load_64bytes_masked(buf + 192, remaining - 192);

        hash0 = _mm512_aesenc_epi128(hash0, data0);
        hash1 = _mm512_aesenc_epi128(hash1, data1);
        hash2 = _mm512_aesenc_epi128(hash2, data2);
        hash3 = _mm512_aesenc_epi128(hash3, data3);
    } else if (remaining >= 128) {
        __m512i data0 = _mm512_loadu_si512((const __m512i*)(buf + 0));
        __m512i data1 = _mm512_loadu_si512((const __m512i*)(buf + 64));
        __m512i data2 = load_64bytes_masked(buf + 128, remaining - 128);

        hash0 = _mm512_aesenc_epi128(hash0, data0);
        hash1 = _mm512_aesenc_epi128(hash1, data1);
        hash2 = _mm512_aesenc_epi128(hash2, data2);
        hash3 = _mm512_aesenc_epi128(hash3, seed512);
    } else if (remaining >= 64) {
        __m512i data0 = _mm512_loadu_si512((const __m512i*)(buf + 0));
        __m512i data1 = load_64bytes_masked(buf + 64, remaining - 64);

        hash0 = _mm512_aesenc_epi128(hash0, data0);
        hash1 = _mm512_aesenc_epi128(hash1, data1);
        hash2 = _mm512_aesenc_epi128(hash2, seed512);
        hash3 = _mm512_aesenc_epi128(hash3, seed512);
    } else if (remaining > 0) {
        __m512i data0 = load_64bytes_masked(buf, remaining);
        hash0 = _mm512_aesenc_epi128(hash0, data0);
        hash1 = _mm512_aesenc_epi128(hash1, seed512);
        hash2 = _mm512_aesenc_epi128(hash2, seed512);
        hash3 = _mm512_aesenc_epi128(hash3, seed512);
    }

    hash0 = _mm512_aesenc_epi128(hash0, seed512);
    hash1 = _mm512_aesenc_epi128(hash1, seed512);
    hash2 = _mm512_aesenc_epi128(hash2, seed512);
    hash3 = _mm512_aesenc_epi128(hash3, seed512);

    hash0 = _mm512_aesenc_epi128(hash0, seed512);
    hash1 = _mm512_aesenc_epi128(hash1, seed512);
    hash2 = _mm512_aesenc_epi128(hash2, seed512);
    hash3 = _mm512_aesenc_epi128(hash3, seed512);

    hash0 = _mm512_aesenc_epi128(hash0, hash2);
    hash1 = _mm512_aesenc_epi128(hash1, hash3);
    hash0 = _mm512_xor_si512(hash0, hash1);

    __m128i h0 = _mm512_extracti32x4_epi32(hash0, 0);
    __m128i h1 = _mm512_extracti32x4_epi32(hash0, 1);
    __m128i h2 = _mm512_extracti32x4_epi32(hash0, 2);
    __m128i h3 = _mm512_extracti32x4_epi32(hash0, 3);
    h1 = _mm_xor_si128(h3, h1);
    h0 = _mm_xor_si128(h2, h0);
    h0 = _mm_xor_si128(h1, h0);

    uint64_t lo = (uint64_t)_mm_cvtsi128_si64(h0);
    uint64_t hi = (uint64_t)_mm_extract_epi64(h0, 1);
    return mum_mix(lo, hi);
}

inline uint64_t hash64(const void * data, size_t len, uint64_t seed)
{
    return hash_long(data, len, seed);
}

}

#endif
