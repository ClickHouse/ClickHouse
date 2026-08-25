#pragma once
// RapidStringHash.h — rapidhash / taotie 接入 ClickHouse 的统一字符串哈希入口。
//
// 分派规则：
//   key.size() <  256  -> rapidhash（纯标量，本机可跑）
//   key.size() >= 256  -> taotie（AVX-512 + VAES，见下）
//
// 调用方（所有 string 哈希都收敛到这里）：
//   StringViewHash（base/base/StringViewHash.h）
//   StringHashTableHash（src/Common/HashTable/StringHashTable.h）

#include <base/RapidHash.h>
#include <base/TaotieHash.h>

#include <cstddef>
#include <cstdint>
#include <string_view>

// seed暂定0
inline constexpr uint64_t kStringHashSeed = 0;

// taotie::hash64 的定义在 <base/TaotieHash.h>（AVX-512 + VAES 门控，签名对齐 rapid::rapidhash）。
// 无 AVX-512 的平台上该头文件展开为空，下面 rapidStringHash 的 #else 分支回退 rapidhash。
inline size_t rapidStringHash(std::string_view key)
{
    const size_t n = key.size();

    if (n < 256)
        return rapid::rapidhash(key.data(), n, kStringHashSeed);

#if defined(__AVX512F__) && defined(__VAES__)
    return taotie::hash64(key.data(), n, kStringHashSeed);
#else
    // 本机无 AVX-512，taotie 编不出来；临时回退 rapidhash 占位。
    return rapid::rapidhash(key.data(), n, kStringHashSeed);
#endif
}
