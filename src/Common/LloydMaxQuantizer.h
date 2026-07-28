#pragma once

#include <base/BFloat16.h>
#include <base/types.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <iterator>

/// 256-level Gaussian Lloyd-Max scalar quantizer (the MSE-optimal scalar quantizer for a standard-normal source).
/// It is intended for values that are approximately N(0, 1) - e.g. a unit-norm embedding coordinate after a random
/// orthogonal (randomized Hadamard) rotation, scaled by sqrt(dimensions) to reach unit variance.
///
/// The code is the index 0..255 of the Lloyd-Max cell, stored as Int8 (index - 128), so the sign bit equals the sign of
/// the value (+0/-0 map to the positive/negative central cells) and the top `b` bits form a valid embedded 2^b-level
/// quantizer: bit-truncation of the code yields coarser Int4/Int2/binary codes.
namespace DB::LloydMax
{

/// Reconstruction levels (sorted ascending; symmetric; index 0..255).
// NOLINTBEGIN(modernize-use-std-numbers): these are quantizer table values that happen to lie near
// math constants such as 1/sqrt(3) and ln(2); they are data, not approximations of those constants.
inline constexpr Float32 LEVELS[256] = {
    -3.94331723f, -3.46399932f, -3.16034096f, -2.93314607f, -2.74992207f, -2.59575919f, -2.46251620f, -2.34523372f,
    -2.24065008f, -2.14649281f, -2.06110438f, -1.98322915f, -1.91188474f, -1.84628084f, -1.78576605f, -1.72979202f,
    -1.67788877f, -1.62964731f, -1.58470730f, -1.54274811f, -1.50348227f, -1.46665067f, -1.43201890f, -1.39937446f,
    -1.36852460f, -1.33929452f, -1.31152588f, -1.28507552f, -1.25981428f, -1.23562592f, -1.21240610f, -1.19006145f,
    -1.16850859f, -1.14767324f, -1.12748941f, -1.10789853f, -1.08884872f, -1.07029404f, -1.05219386f, -1.03451219f,
    -1.01721718f, -1.00028055f, -0.98367719f, -0.96738473f, -0.95138317f, -0.93565460f, -0.92018290f, -0.90495349f,
    -0.88995319f, -0.87516997f, -0.86059283f, -0.84621168f, -0.83201723f, -0.81800086f, -0.80415460f, -0.79047101f,
    -0.77694313f, -0.76356447f, -0.75032892f, -0.73723071f, -0.72426444f, -0.71142496f, -0.69870743f, -0.68610723f,
    -0.67362001f, -0.66124158f, -0.64896799f, -0.63679545f, -0.62472037f, -0.61273927f, -0.60084886f, -0.58904597f,
    -0.57732756f, -0.56569073f, -0.55413266f, -0.54265067f, -0.53124215f, -0.51990462f, -0.50863566f, -0.49743295f,
    -0.48629424f, -0.47521736f, -0.46420020f, -0.45324075f, -0.44233703f, -0.43148712f, -0.42068918f, -0.40994142f,
    -0.39924207f, -0.38858944f, -0.37798188f, -0.36741778f, -0.35689557f, -0.34641372f, -0.33597074f, -0.32556517f,
    -0.31519559f, -0.30486060f, -0.29455885f, -0.28428900f, -0.27404974f, -0.26383980f, -0.25365792f, -0.24350287f,
    -0.23337343f, -0.22326841f, -0.21318664f, -0.20312698f, -0.19308828f, -0.18306942f, -0.17306929f, -0.16308682f,
    -0.15312092f, -0.14317053f, -0.13323459f, -0.12331206f, -0.11340192f, -0.10350313f, -0.09361469f, -0.08373558f,
    -0.07386480f, -0.06400137f, -0.05414428f, -0.04429256f, -0.03444523f, -0.02460130f, -0.01475981f, -0.00491977f,
    0.00491977f, 0.01475981f, 0.02460130f, 0.03444523f, 0.04429256f, 0.05414428f, 0.06400137f, 0.07386480f,
    0.08373558f, 0.09361469f, 0.10350313f, 0.11340192f, 0.12331206f, 0.13323459f, 0.14317053f, 0.15312092f,
    0.16308682f, 0.17306929f, 0.18306942f, 0.19308828f, 0.20312698f, 0.21318664f, 0.22326841f, 0.23337343f,
    0.24350287f, 0.25365792f, 0.26383980f, 0.27404974f, 0.28428900f, 0.29455885f, 0.30486060f, 0.31519559f,
    0.32556517f, 0.33597074f, 0.34641372f, 0.35689557f, 0.36741778f, 0.37798188f, 0.38858944f, 0.39924207f,
    0.40994142f, 0.42068918f, 0.43148712f, 0.44233703f, 0.45324075f, 0.46420020f, 0.47521736f, 0.48629424f,
    0.49743295f, 0.50863566f, 0.51990462f, 0.53124215f, 0.54265067f, 0.55413266f, 0.56569073f, 0.57732756f,
    0.58904597f, 0.60084886f, 0.61273927f, 0.62472037f, 0.63679545f, 0.64896799f, 0.66124158f, 0.67362001f,
    0.68610723f, 0.69870743f, 0.71142496f, 0.72426444f, 0.73723071f, 0.75032892f, 0.76356447f, 0.77694313f,
    0.79047101f, 0.80415460f, 0.81800086f, 0.83201723f, 0.84621168f, 0.86059283f, 0.87516997f, 0.88995319f,
    0.90495349f, 0.92018290f, 0.93565460f, 0.95138317f, 0.96738473f, 0.98367719f, 1.00028055f, 1.01721718f,
    1.03451219f, 1.05219386f, 1.07029404f, 1.08884872f, 1.10789853f, 1.12748941f, 1.14767324f, 1.16850859f,
    1.19006145f, 1.21240610f, 1.23562592f, 1.25981428f, 1.28507552f, 1.31152588f, 1.33929452f, 1.36852460f,
    1.39937446f, 1.43201890f, 1.46665067f, 1.50348227f, 1.54274811f, 1.58470730f, 1.62964731f, 1.67788877f,
    1.72979202f, 1.78576605f, 1.84628084f, 1.91188474f, 1.98322915f, 2.06110438f, 2.14649281f, 2.24065008f,
    2.34523372f, 2.46251620f, 2.59575919f, 2.74992207f, 2.93314607f, 3.16034096f, 3.46399932f, 3.94331723f,
};

/// 255 internal decision boundaries (sorted). The cell index of x is the number of boundaries < x.
inline constexpr Float32 BOUNDARIES[255] = {
    -3.70365827f, -3.31217014f, -3.04674351f, -2.84153407f, -2.67284063f, -2.52913769f, -2.40387496f, -2.29294190f,
    -2.19357144f, -2.10379860f, -2.02216677f, -1.94755695f, -1.87908279f, -1.81602345f, -1.75777904f, -1.70384040f,
    -1.65376804f, -1.60717730f, -1.56372770f, -1.52311519f, -1.48506647f, -1.44933479f, -1.41569668f, -1.38394953f,
    -1.35390956f, -1.32541020f, -1.29830070f, -1.27244490f, -1.24772010f, -1.22401601f, -1.20123378f, -1.17928502f,
    -1.15809091f, -1.13758133f, -1.11769397f, -1.09837363f, -1.07957138f, -1.06124395f, -1.04335303f, -1.02586469f,
    -1.00874887f, -0.99197887f, -0.97553096f, -0.95938395f, -0.94351889f, -0.92791875f, -0.91256819f, -0.89745334f,
    -0.88256158f, -0.86788140f, -0.85340225f, -0.83911445f, -0.82500904f, -0.81107773f, -0.79731280f, -0.78370707f,
    -0.77025380f, -0.75694669f, -0.74377981f, -0.73074757f, -0.71784470f, -0.70506619f, -0.69240733f, -0.67986362f,
    -0.66743079f, -0.65510478f, -0.64288172f, -0.63075791f, -0.61872982f, -0.60679406f, -0.59494741f, -0.58318677f,
    -0.57150915f, -0.55991170f, -0.54839166f, -0.53694641f, -0.52557339f, -0.51427014f, -0.50303431f, -0.49186359f,
    -0.48075580f, -0.46970878f, -0.45872048f, -0.44778889f, -0.43691208f, -0.42608815f, -0.41531530f, -0.40459174f,
    -0.39391575f, -0.38328566f, -0.37269983f, -0.36215668f, -0.35165464f, -0.34119223f, -0.33076795f, -0.32038038f,
    -0.31002809f, -0.29970972f, -0.28942392f, -0.27916937f, -0.26894477f, -0.25874886f, -0.24858040f, -0.23843815f,
    -0.22832092f, -0.21822753f, -0.20815681f, -0.19810763f, -0.18807885f, -0.17806935f, -0.16807806f, -0.15810387f,
    -0.14814572f, -0.13820256f, -0.12827333f, -0.11835699f, -0.10845253f, -0.09855891f, -0.08867513f, -0.07880019f,
    -0.06893308f, -0.05907282f, -0.04921842f, -0.03936890f, -0.02952327f, -0.01968056f, -0.00983979f, 0.00000000f,
    0.00983979f, 0.01968056f, 0.02952327f, 0.03936890f, 0.04921842f, 0.05907282f, 0.06893308f, 0.07880019f,
    0.08867513f, 0.09855891f, 0.10845253f, 0.11835699f, 0.12827333f, 0.13820256f, 0.14814572f, 0.15810387f,
    0.16807806f, 0.17806935f, 0.18807885f, 0.19810763f, 0.20815681f, 0.21822753f, 0.22832092f, 0.23843815f,
    0.24858040f, 0.25874886f, 0.26894477f, 0.27916937f, 0.28942392f, 0.29970972f, 0.31002809f, 0.32038038f,
    0.33076795f, 0.34119223f, 0.35165464f, 0.36215668f, 0.37269983f, 0.38328566f, 0.39391575f, 0.40459174f,
    0.41531530f, 0.42608815f, 0.43691208f, 0.44778889f, 0.45872048f, 0.46970878f, 0.48075580f, 0.49186359f,
    0.50303431f, 0.51427014f, 0.52557339f, 0.53694641f, 0.54839166f, 0.55991170f, 0.57150915f, 0.58318677f,
    0.59494741f, 0.60679406f, 0.61872982f, 0.63075791f, 0.64288172f, 0.65510478f, 0.66743079f, 0.67986362f,
    0.69240733f, 0.70506619f, 0.71784470f, 0.73074757f, 0.74377981f, 0.75694669f, 0.77025380f, 0.78370707f,
    0.79731280f, 0.81107773f, 0.82500904f, 0.83911445f, 0.85340225f, 0.86788140f, 0.88256158f, 0.89745334f,
    0.91256819f, 0.92791875f, 0.94351889f, 0.95938395f, 0.97553096f, 0.99197887f, 1.00874887f, 1.02586469f,
    1.04335303f, 1.06124395f, 1.07957138f, 1.09837363f, 1.11769397f, 1.13758133f, 1.15809091f, 1.17928502f,
    1.20123378f, 1.22401601f, 1.24772010f, 1.27244490f, 1.29830070f, 1.32541020f, 1.35390956f, 1.38394953f,
    1.41569668f, 1.44933479f, 1.48506647f, 1.52311519f, 1.56372770f, 1.60717730f, 1.65376804f, 1.70384040f,
    1.75777904f, 1.81602345f, 1.87908279f, 1.94755695f, 2.02216677f, 2.10379860f, 2.19357144f, 2.29294190f,
    2.40387496f, 2.52913769f, 2.67284063f, 2.84153407f, 3.04674351f, 3.31217014f, 3.70365827f,
};
// NOLINTEND(modernize-use-std-numbers)

/// Quantize a value (expected to be ~N(0, 1)) to its Lloyd-Max cell index stored as Int8 (index - 128). The central
/// decision boundary is exactly 0; the tie is broken by sign so the code's sign bit always matches the input's sign
/// (+0 -> positive central cell, -0 -> negative). A NaN maps to the positive central cell.
inline Int8 quantize(Float32 x)
{
    size_t index = 0;
    if (std::isnan(x))
        index = 128;
    else if (x == 0.0f)
        index = std::signbit(x) ? 127 : 128;
    else
        index = static_cast<size_t>(std::lower_bound(std::begin(BOUNDARIES), std::end(BOUNDARIES), x) - std::begin(BOUNDARIES));
    return static_cast<Int8>(static_cast<Int16>(index) - 128);
}

/// Reconstruct the cell's reconstruction level from an Int8 code produced by `quantize`.
inline Float32 dequantize(Int8 code)
{
    return LEVELS[static_cast<size_t>(static_cast<Int16>(code) + 128)];
}

/// Direct lookup table keyed by the raw BFloat16 bit pattern. BFloat16 has only 65536 possible
/// values, so the whole quantizer is precomputed once: at runtime quantization is a single load
/// indexed by the bits, with no Float32 conversion or boundary search. Built lazily on first use.
inline const std::array<Int8, 65536> & quantizeCodeLUT()
{
    static const std::array<Int8, 65536> lut = []
    {
        std::array<Int8, 65536> table{};
        for (UInt32 bits = 0; bits <= 0xFFFFu; ++bits)
        {
            const Float32 x = static_cast<Float32>(BFloat16::fromBits(static_cast<UInt16>(bits)));
            size_t index = 0;
            if (std::isnan(x))
                index = 128;
            else if (x == 0.0f)
                /// The central decision boundary is exactly 0; break the tie by sign so the sign
                /// bit always matches the input (+0 -> positive central cell, -0 -> negative).
                index = std::signbit(x) ? 127 : 128;
            else
                index = static_cast<size_t>(std::lower_bound(std::begin(BOUNDARIES), std::end(BOUNDARIES), x) - std::begin(BOUNDARIES));
            table[bits] = static_cast<Int8>(static_cast<Int16>(index) - 128);
        }
        return table;
    }();
    return lut;
}

/// Precomputed BFloat16 reconstruction levels (the bf16 representations of the 256 Lloyd-Max levels).
/// Dequantization is then a direct BFloat16 (i.e. UInt16) copy from this table, with no Float32
/// conversion at runtime. Built lazily on first use.
inline const std::array<BFloat16, 256> & dequantizeLevels()
{
    static const std::array<BFloat16, 256> levels = []
    {
        std::array<BFloat16, 256> table{};
        for (size_t i = 0; i < 256; ++i)
            table[i] = BFloat16(LEVELS[i]);
        return table;
    }();
    return levels;
}

/// Reconstruction levels, as `Float32`, keyed directly by the raw code byte (the untransposed `QBit(Int8)` byte, which is
/// `index XOR 0x80`), for every precision `1..8`. Used by the `...TransposedQuantized` distance functions to dequantize a
/// `QBit(Int8)` on the fly. Row `p` reconstructs a code truncated to its top `p` bits (a `2^p`-level embedded quantizer):
/// the low `8 - p` bits are zero, and the missing bits are filled to the coarse cell's centre so that a lower precision
/// still reconstructs a representative value rather than the cell's lower edge. Row `8` is the exact per-cell level and
/// equals `toFloat32(dequantizeInt8ToBFloat16(code))`. Row `0` is unused (precision `0` is invalid). Built lazily on first use.
inline const std::array<std::array<Float32, 256>, 9> & transposedDequantLUT()
{
    static const std::array<std::array<Float32, 256>, 9> lut = []
    {
        std::array<std::array<Float32, 256>, 9> table{};
        for (size_t precision = 1; precision <= 8; ++precision)
        {
            for (size_t raw = 0; raw < 256; ++raw)
            {
                /// Keep the top `precision` bits of the code byte; drop the rest.
                const auto top = static_cast<uint8_t>(raw & (0xFFu << (8 - precision)));
                /// Round to the centre of the coarse cell by setting the most significant dropped bit (a no-op at precision 8).
                const auto centre = static_cast<uint8_t>(precision < 8 ? (top | (1u << (7 - precision))) : top);
                /// The raw code byte is `index XOR 0x80`, so recover the level index by flipping the top bit back.
                const auto index = static_cast<uint8_t>(centre ^ 0x80u);
                table[precision][raw] = static_cast<Float32>(BFloat16(LEVELS[index]));
            }
        }
        return table;
    }();
    return lut;
}

}
