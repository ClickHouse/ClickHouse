#pragma once
// RapidHash.h — rapidhash 的独立单文件版（header-only）
//
// 来源: smhasher-main/hashes/rapidhash.cpp
//       作者 Nicolas De Carli, 2024, 基于 wyhash, BSD-2 许可
// 本文件只保留核心算法，去掉了 smhasher 的框架宏（Platform.h / GET_U64 / MathMult 等），
// 可以直接 #include 进任何项目（包括之后的 ClickHouse）。
//
// 编译要求: g++ / clang++（用了 64x64 -> 128 位的 __int128）。
//           MSVC 需改用 _umul128，本任务用 MinGW/g++ 即可。
//
// 算法速览（配合逐行阅读）:
//   1. seed 初始化:  seed ^= mix(seed ^ secret[0], secret[1]) ^ len
//   2. 主体分三档:  len<=16 直接拼字 / 17..48 用 mix 收尾 / >48 每 48 字节一卷
//   3. 收尾:        mum(a,b) 后 mix，把长度和秘密常量都搅进去
// 核心算子: mum = 64x64->128 乘法取高低 64 位; mix = mum 后高低异或。

#include <cstdint>
#include <cstring>

namespace rapid {

static constexpr uint64_t kSecret[3] = {
    UINT64_C(0x2d358dccaa6c78a5),
    UINT64_C(0x8bb84b93962eacc9),
    UINT64_C(0x4b33a62ed433d4a3),
};

// 64x64 -> 128 位乘法，lo / hi 分别取低 / 高 64 位。
static inline void mul128(uint64_t & lo, uint64_t & hi, uint64_t a, uint64_t b)
{
    unsigned __int128 p = (unsigned __int128)a * b;
    lo = (uint64_t)p;
    hi = (uint64_t)(p >> 64);
}

// MUM: 用 128 位积的低/高 64 位替换 a/b。
static inline void mum(uint64_t & a, uint64_t & b)
{
    uint64_t lo, hi;
    mul128(lo, hi, a, b);
    a = lo;
    b = hi;
}

// MIX: mum 之后高低异或（这是 rapidhash 的基本搅拌单元）。
static inline uint64_t mix(uint64_t a, uint64_t b)
{
    mum(a, b);
    return a ^ b;
}

// 无对齐小端读（x86-64 上等价于直接 load，用 memcpy 避免未对齐 UB）。
static inline uint64_t read64(const void * p)
{
    uint64_t v;
    std::memcpy(&v, p, 8);
    return v;
}

static inline uint32_t read32(const void * p)
{
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}

// 1..3 字节的小 key：把首/中/尾三个字节散到 64 位的高/中/低位置。
static inline uint64_t readSmall(const uint8_t * p, size_t k)
{
    return ((uint64_t)p[0] << 56) | ((uint64_t)p[k >> 1] << 32) | p[k - 1];
}

// rapidhash 主函数（64 位输出；非 protected、非 unrolled 的参考版）。
inline uint64_t rapidhash(const void * key, size_t len, uint64_t seed)
{
    const uint8_t * p = static_cast<const uint8_t *>(key);
    uint64_t a, b;

    seed ^= mix(seed ^ kSecret[0], kSecret[1]) ^ len;

    if (len <= 16)
    {
        if (len >= 4)
        {
            const uint8_t * plast = p + len - 4;
            a = ((uint64_t)read32(p) << 32) | read32(plast);
            const uint64_t delta = (len & 24) >> (len >> 3);
            b = ((uint64_t)read32(p + delta) << 32) | read32(plast - delta);
        }
        else if (len > 0)
        {
            a = readSmall(p, len);
            b = 0;
        }
        else
        {
            a = b = 0;
        }
    }
    else
    {
        size_t i = len;
        if (i > 48)
        {
            uint64_t see1 = seed, see2 = seed;
            do
            {
                seed = mix(read64(p)      ^ kSecret[0], read64(p +  8) ^ seed);
                see1 = mix(read64(p + 16) ^ kSecret[1], read64(p + 24) ^ see1);
                see2 = mix(read64(p + 32) ^ kSecret[2], read64(p + 40) ^ see2);
                p += 48;
                i -= 48;
            } while (i >= 48);
            seed ^= see1 ^ see2;
        }
        if (i > 16)
        {
            seed = mix(read64(p) ^ kSecret[2], read64(p + 8) ^ seed ^ kSecret[1]);
            if (i > 32)
                seed = mix(read64(p + 16) ^ kSecret[2], read64(p + 24) ^ seed);
        }
        a = read64(p + i - 16);
        b = read64(p + i -  8);
    }

    a ^= kSecret[1];
    b ^= seed;
    mum(a, b);
    return mix(a ^ kSecret[0] ^ len, b ^ kSecret[1]);
}

} // namespace rapid
