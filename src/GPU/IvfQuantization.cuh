#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>

/// The 4-bit quantization of the `ivf` index: the encoding of a row on the host and the approximate distance to it on the
/// device. Plain types only: both ClickHouse and the GPU island of cuVS include it.
///
/// A row x of the list of the centroid c is stored as its rotated residual r = R (x - c), every component as
/// `lo + code * step` with a code in [0, 15]:
///
///     lo = min(r), step = (max(r) - lo) / 15, code_i = round((r_i - lo) / step)
///
/// For a query q and u = R q - R c, the distance to the decoded row x_hat needs one dot product with the codes:
///
///     |q - x_hat|^2 = |u - r_hat|^2 = |u|^2 - 2 (lo sum(u) + step dot(u, code)) + |r_hat|^2
namespace DB::GPU::IvfQuantization
{

/// What a row stores besides its codes.
struct CodeParams
{
    float lo;
    float step;
    /// |r_hat|^2 of the decoded residual.
    float squared_length;
};

/// The codes of a row are 16-byte words of 32 components each, read whole by the device: component i is the nibble
/// i % 8 of the 32-bit part (i % 32) / 8 of the word i / 32.
constexpr size_t codeWords(size_t dimensions)
{
    return (dimensions + 31) / 32;
}

constexpr size_t codeBytes(size_t dimensions)
{
    return codeWords(dimensions) * 16;
}

constexpr size_t codeByte(size_t component)
{
    return (component / 32) * 16 + ((component % 32) / 8) * 4 + (component % 8) / 2;
}

constexpr unsigned codeShift(size_t component)
{
    return 4 * (component % 2);
}

/// A list stores its rows, then their `CodeParams`, then their codes, each as one array.
constexpr size_t entryBytes(size_t dimensions)
{
    return sizeof(uint32_t) + sizeof(CodeParams) + codeBytes(dimensions);
}

/// Writes the codes of the residual `r` to `codes` (`codeBytes(dimensions)` bytes).
inline CodeParams encode(const float * r, size_t dimensions, uint8_t * codes)
{
    float lo = r[0];
    float hi = r[0];
    for (size_t i = 1; i < dimensions; ++i)
    {
        lo = std::min(lo, r[i]);
        hi = std::max(hi, r[i]);
    }
    const float step = hi > lo ? (hi - lo) / 15 : 1.0f;

    std::fill(codes, codes + codeBytes(dimensions), 0);
    float squared_length = 0;
    for (size_t i = 0; i < dimensions; ++i)
    {
        const int code = std::clamp(static_cast<int>(std::lround((r[i] - lo) / step)), 0, 15);
        const float decoded = lo + static_cast<float>(code) * step;
        squared_length += decoded * decoded;
        codes[codeByte(i)] |= static_cast<uint8_t>(code << codeShift(i));
    }
    return {lo, step, squared_length};
}

#if defined(__CUDACC__)

/// The query side of a (query, list): u = R q - R c, padded with zeros to the components of the codes.
struct QueryResidual
{
    const float * u;
    float squared_length;
    float sum;
};

/// A list in memory, see `entryBytes`.
struct ListView
{
    const uint32_t * rows;
    const CodeParams * params;
    const uint4 * codes;
    uint32_t size;

    __device__ ListView(const char * begin, uint32_t size_)
        : rows(reinterpret_cast<const uint32_t *>(begin))
        , params(reinterpret_cast<const CodeParams *>(begin + size_t(size_) * sizeof(uint32_t)))
        , codes(reinterpret_cast<const uint4 *>(begin + size_t(size_) * (sizeof(uint32_t) + sizeof(CodeParams))))
        , size(size_)
    {
    }
};

/// dot(u, code) of a row whose codes start at `codes`.
__device__ inline float dotWithCodes(const float * u, const uint4 * codes, size_t words)
{
    float dot = 0;
    for (size_t w = 0; w < words; ++w)
    {
        const uint4 word = codes[w];
        const uint32_t parts[4] = {word.x, word.y, word.z, word.w};
#pragma unroll
        for (int part = 0; part < 4; ++part)
#pragma unroll
            for (int nibble = 0; nibble < 8; ++nibble)
                dot += u[w * 32 + part * 8 + nibble] * float((parts[part] >> (4 * nibble)) & 15u);
    }
    return dot;
}

__device__ inline float estimateDistance(const QueryResidual & query, const CodeParams & params, float dot)
{
    return query.squared_length - 2.0f * (params.lo * query.sum + params.step * dot) + params.squared_length;
}

#endif

}
