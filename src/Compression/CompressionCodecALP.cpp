#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>

#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h>
#include <base/unaligned.h>

#include <Compression/FFOR.h>

#include <array>

#include <boost/algorithm/string.hpp>

#include "iostream"

namespace DB
{
/**
 * ALP (Adaptive Lossless floating-Point) compression codec.
 *
 * This implementation is based on the ALP paper (https://ir.cwi.nl/pub/33334) and implements both ALP variants for Float32 and Float64.
 * Standard APL variant encodes floating-point values as scaled integers (plus a small set of exceptions), then applies Frame-of-Reference + bit-packing.
 * RD (real doubles) ALP variant encodes doubles as a pair of integers (left and right bits) and applies dictionary encoding to the left parts.
 *
 * Overall Stream Layout
 *   [ALP codec header]
 *   [ALP block 0]
 *   [ALP block 1]
 *   ...
 *   [ALP block N-1]
 *
 * ALP codec header (4 bytes):
 *   - meta byte (1 byte):
 *     - bits 0-3: codec version (currently 1)
 *     - bit 4:    variant flag (0 = ALP (STD), 1 = ALP RD)
 *     - bits 5-7: reserved (must be 0)
 *   - float width (1 byte):
 *     - 4 or 8 bytes (Float32 / Float64)
 *   - block float count (2 bytes):
 *     - number of floats per block (UInt16), currently fixed to 1024, other values are not supported.
 *
 * The input column is split into blocks of up to ALP_BLOCK_MAX_FLOAT_COUNT values (1024).
 * Each block is encoded independently and can be either compressed or left raw, depending on the estimated gain.
 *
 * ---
 *
 * STANDARD ALP Variant (STD)
 * Core Idea: Decimal-Based Integerization
 * The ALP paper observes that most stored doubles originate as decimals. For a block, we try to represent each float value as an integer:
 *   d = (Int64) round(v * 10^e * 10^(-f))
 * where:
 *   - v is the original floating-point value
 *   - d is the encoded integer
 *   - e controls the decimal scaling up
 *   - f controls how many trailing decimal zeros we effectively “cut off” again
 * A value is considered exactly encodable if:
 *   decodeValue(encodeValue(v, e, f), e, f) == v
 * Encodable values become part of the integer stream; non-encodable values become exceptions stored verbatim.
 * Encodable eligibility is based on bit-exact equality, not epsilon-based closeness, because the codec is lossless.
 *
 * Two-Level Sampling to Select (e,f)
 * ALP’s adaptivity over a block is driven by a two-level sampling scheme:
 *   1) Global pre-sampling over the entire column:
 *     - Take ALP_PARAMS_ESTIMATION_SAMPLES disjoint sub-samples from the column (each up to ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS values).
 *     - For each sub-sample, brute-force over all valid (e,f) pairs with 0 <= e < |EXPONENTS| and 0 <= f <= e.
 *     - For each (e,f), estimate the encoded size and keep track of the best (e,f) for that sub-sample.
 *     - The result is a small, global candidate set of (e,f) pairs (up to 5) that are likely good for this column.
 *   2) Per-block refinement
 *     - For each block of up to 1024 values, take a local sample
 *     - Evaluate only the global candidate (e,f) list for that sample to choose the best (e,f) for that block.
 *
 * Per-Block Encoding Schema (Compressed Case)
 *   - 1 byte: exponent (e) (UInt8)
 *   - 1 byte: fraction (f) (UInt8)
 *   - 2 bytes: exceptions count (UInt16)
 *   - 1 byte:  FOR bit-width (UInt8)
 *   - 8 bytes: FOR base value (Int64)
 *   - Compressed block payload:
 *     * Bit-packed values: (encoded_value - FOR_base) for each successfully encoded float, using bit-width bits per value.
 *     * Exceptions (for values that couldn't round-trip losslessly):
 *       - UInt16 index (position in source block - offset)
 *       - raw value (float or double)
 *
 * Per-Block Encoding Schema (Uncompressed Case)
 *   - 1 byte: 255 (uncompressed block marker)
 *   - Raw numbers for the block
 *
 * ---
 *
 * REAL DOUBLES ALP Variant (RD)
 * Core Idea: Bit-Split with Dictionary Encoding of the High Bits
 * Instead of treating values as decimals, it reinterprets every floating-point value as an unsigned integer (32/64-bit)
 * and splits the bit pattern into two parts at a chosen cut point `left_bits` (1–16) and `right_bits` (remaining bits):
 *
 *   value_bits = left_part (top left_bits bits) | right_part (remaining right_bits bits)
 *
 * The key observation is that in many float columns the high bits-sign, exponent, and top mantissa - repeat heavily across values in a block.
 * The left parts therefore form a small vocabulary that can be represented as a compact dictionary (up to 8 entries), while the right parts are bit-packed directly.
 *
 * Per-Block left_bits Selection
 * For each block, the encoder searches for the `left_bits` split point that minimises the estimated encoded block size.
 * It samples up to ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS values evenly from the block and evaluates every candidate left_bits from 1 to ALP_RD_CUTTING_LIMIT (16).
 *
 * Per-Block Encoding Schema (Compressed Case)
 *   - 1 byte:  left bit-width (UInt8, valid range 1–16)
 *   - 1 byte:  dictionary size (Int8, valid range 0-8)
 *   - 2 bytes: exception count (UInt16)
 *   - Dictionary: dict_size × 2 bytes (UInt16), each a left-part bit pattern, ordered by frequency descending (index 0 = most frequent)
 *   - Bit-packed dictionary indices: FFOR payload, "bit-width of dictionary size" bits per slot, always sized for ALP_BLOCK_MAX_FLOAT_COUNT (1024) slots and base 0
 *   - Bit-packed right parts: FFOR payload, right bits per slot, always sized for ALP_BLOCK_MAX_FLOAT_COUNT (1024) slots and base 0
 *   - Exceptions (one per non-dictionary value):
 *       - UInt16 index (position in source block - offset)
 *       - raw value (float or double)
 *
 * Per-Block Encoding Schema (Uncompressed Case)
 *   - 1 byte: 255 (uncompressed block marker)
 *   - Raw numbers for the block
 *
 * ---
 *
 * Notes
 *   - Supported types: 4 and 8 bytes floating point.
 *   - The scheme is fully lossless: all non-exception values are proven round-trip encodable; all others are stored as raw exceptions.
 */
class CompressionCodecALP final : public ICompressionCodec
{
public:
    enum class Variant : UInt8
    {
        STD = 0,
        RD = 1
    };

    explicit CompressionCodecALP(UInt8 float_width_, Variant variant_);
    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;
protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;
    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    bool isFloatingPointTimeSeriesCodec() const override { return true; }
    bool isExperimental() const override { return true; }
    String getDescription() const override;
private:
    UInt8 float_width;
    Variant variant;
};

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
}

namespace
{
constexpr UInt8 ALP_CODEC_VERSION = 1;

/**
 * ALP codec header size in bytes: meta (1) + float width (1) + block float count (2)
 */
constexpr UInt32 ALP_CODEC_HEADER_SIZE = 2 * sizeof(UInt8) + sizeof(UInt16);

/**
 * ALP block header size in bytes: exponent (1) + fraction (1) + exception count (2) + bit-width (1) + FOR base (8)
 */
constexpr UInt32 ALP_BLOCK_HEADER_SIZE = 3 * sizeof(UInt8) + sizeof(UInt16) + sizeof(Int64);

constexpr UInt32 ALP_UNENCODED_BLOCK_HEADER_SIZE = sizeof(UInt8);
constexpr UInt8 ALP_UNENCODED_BLOCK_EXPONENT = 255;

/**
 * Maximum number of floats per ALP block. Reference FastLanes implementation uses 1024 values only.
 * Exactly the same size is used for FastLanes FFOR.
 * Changing this value affects compatibility with FastLanes, and it likely won't auto-vectorise. Don't do it.
 */
constexpr UInt32 ALP_BLOCK_MAX_FLOAT_COUNT = Compression::FFOR::DEFAULT_VALUES;

constexpr UInt32 ALP_PARAMS_ESTIMATION_SAMPLES = 8;
constexpr UInt32 ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS = 32;
constexpr UInt32 ALP_PARAMS_ESTIMATION_CANDIDATES = 5;

/**
 * ALP RD block header size in bytes: left bit-width (1) + dictionary size (1) + exception count (2)
 */
constexpr UInt32 ALP_RD_BLOCK_HEADER_SIZE = 2 * sizeof(UInt8) + sizeof(UInt16);

constexpr UInt32 ALP_RD_UNENCODED_BLOCK_HEADER_SIZE = sizeof(UInt8);
constexpr UInt8 ALP_RD_UNENCODED_BLOCK_BIT_WIDTH = 255;

constexpr UInt8 ALP_RD_CUTTING_LIMIT = 16;
/**
 * Limit the dictionary size to 8 entries (3 bits to encode the dictionary index).
 */
constexpr UInt8 ALP_RD_MAX_DICT_BITS = 3;
constexpr UInt8 ALP_RD_MAX_DICT_SIZE = (1 << ALP_RD_MAX_DICT_BITS); // 8

/**
 * Alignment for internal ALP buffers. Used to align buffers for FFOR encoding/decoding.
 * \note FastLanes FFOR requires 64-byte alignment for optimal performance.
 */
constexpr UInt8 ALP_BUFFER_ALIGNMENT = 64;

template <typename T>
concept FLOAT = std::is_same_v<T, Float32> || std::is_same_v<T, Float64>;

template <FLOAT T>
struct ALPFloatTraits;

template <FLOAT T, UInt32 exponent_count, bool inverse>
constexpr std::array<T, exponent_count> generatePowersOf10()
{
    std::array<T, exponent_count> arr{};
    for (UInt64 i = 0, v = 1; i < exponent_count; ++i, v *= 10)
        arr[i] = inverse ? static_cast<T>(1) / static_cast<T>(v) : static_cast<T>(v);
    return arr;
}

/**
 * ALP Float64 parameters.
 *
 * Float64 scaling is limited to 10^18 and inputs are clamped to ±9223372036854773760 (2^63 − 2048).
 * In this range, a meaningful subset of values still "survives" scale by 10^e → round → cast to Int64.
 * The reference implementation use 9223372036854774784 (2^63 − 1024), which led to integer overflow after rounding (num + magic - magic).
 */
template<>
struct ALPFloatTraits<Float64>
{
    static constexpr UInt8 EXPONENT_COUNT = 19;

    static constexpr std::array<Float64, EXPONENT_COUNT> EXPONENTS = generatePowersOf10<Float64, EXPONENT_COUNT, false>();
    static constexpr std::array<Float64, EXPONENT_COUNT> FRACTIONS = generatePowersOf10<Float64, EXPONENT_COUNT, true>();

    static constexpr Float64 UPPER = 9223372036854773760.0;
    static constexpr Float64 LOWER = -9223372036854773760.0;

    static constexpr Float64 ROUND_MAGIC = 6755399441055744.0; // 2^51 + 2^52
};

/**
 * ALP Float32 parameters.
 *
 * Float32 scaling is limited to 10^9 and inputs are clamped to ±9223371487098961920.
 * In this range, a meaningful subset of values still "survives" scale by 10^e → round → cast to Int64.
 *
 * The UPPER value is the largest Float32 value that has an exact Int64 representation.
 * Higher values would overflow when cast to Int64 after Float32 rounding (num + magic - magic).
 */
template<>
struct ALPFloatTraits<Float32>
{
    static constexpr UInt8 EXPONENT_COUNT = 10;

    static constexpr std::array<Float32, EXPONENT_COUNT> EXPONENTS = generatePowersOf10<Float32, EXPONENT_COUNT, false>();
    static constexpr std::array<Float32, EXPONENT_COUNT> FRACTIONS = generatePowersOf10<Float32, EXPONENT_COUNT, true>();

    static constexpr Float32 UPPER = 9223371487098961920.0f;
    static constexpr Float32 LOWER = -9223371487098961920.0f;

    static constexpr Float32 ROUND_MAGIC = 12582912.0f; // 2^22 + 2^23
};

template<FLOAT T>
struct ALPFloatUtils
{
    static Int64 encodeValue(T value, UInt8 exponent, UInt8 fraction)
    {
        // Scale float to integer, it is important to keep two multiplication steps to comply with the ALP paper and reference implementation
        T value_enc = value * ALPFloatTraits<T>::EXPONENTS[exponent] * ALPFloatTraits<T>::FRACTIONS[fraction];

        const bool invalid = std::isinf(value_enc) ||
            std::isnan(value_enc) ||
                value_enc < ALPFloatTraits<T>::LOWER || value_enc > ALPFloatTraits<T>::UPPER ||
                    (value_enc == static_cast<T>(0.0) && std::signbit(value_enc));

        if (unlikely(invalid))
            return static_cast<Int64>(ALPFloatTraits<T>::UPPER);

        // Fast rounding to integer by adding and subtracting a large constant (IEEE-754 mantissa limit)
        value_enc = value_enc + ALPFloatTraits<T>::ROUND_MAGIC - ALPFloatTraits<T>::ROUND_MAGIC;
        return static_cast<Int64>(value_enc);
    }

    static T decodeValue(Int64 value, UInt8 exponent, UInt8 fraction)
    {
        T value_float = static_cast<T>(value);
        // Scale back to float, it is important to keep two multiplication steps to comply with the ALP paper and reference implementation
        T value_dec = value_float * ALPFloatTraits<T>::EXPONENTS[fraction] * ALPFloatTraits<T>::FRACTIONS[exponent];
        return value_dec;
    }
};

template <FLOAT T>
struct ALPUtils
{
    /**
     * Calculate the number of bits required to encode the given value, based on the position of the most significant bit.
     * Returns 0 for value 0, otherwise returns the position of the highest set bit.
     */
    static UInt8 calculateBitWidth(const UInt64 value)
    {
        if (unlikely(value == 0))
            return 0;

        const auto bits = sizeof(UInt64) * 8 - getLeadingZeroBitsUnsafe<UInt64>(value);
        return static_cast<UInt8>(bits);
    }

    /**
     * Write an unencoded block to the destination buffer.
     * Returns the pointer to the end of the written block.
     */
    static char * writeUnencoded(const char * source, const UInt16 float_count, char * dest)
    {
        const size_t block_size = float_count * sizeof(T);
        memcpy(dest, source, block_size);
        dest += block_size;

        return dest;
    }

    /**
     * Decompress an unencoded block from the source buffer to the destination buffer.
     * Updates the source and destination pointers to the end of the read and written data, respectively.
     * Throws an exception if the source or destination buffers do not have enough space for the unencoded block.
     */
    static void decompressUnencodedBlock(const char * & source, const char * source_end, char * & dest, const char * dest_end, const UInt16 float_count)
    {
        const size_t block_size = float_count * sizeof(T);
        if (unlikely(source + block_size > source_end || dest + block_size > dest_end))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP-encoded data, incomplete uncompressed block");

        memcpy(dest, source, block_size);
        source += block_size;
        dest += block_size;
    }
};

template <FLOAT T>
class ALPCodecEncoder
{
public:
    ALPCodecEncoder() = default;

    UInt32 encode(const char * source, const UInt32 source_size, char * dest)
    {
        if (source_size % sizeof(T) != 0)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with ALP codec, data size {} is not aligned to {}", source_size, sizeof(T));

        UInt32 float_count = source_size / sizeof(T);

        estimateParamCandidates(source, float_count);

        const char * dest_start = dest;

        while (float_count >= ALP_BLOCK_MAX_FLOAT_COUNT)
        {
            dest = encodeBlock(source, ALP_BLOCK_MAX_FLOAT_COUNT, dest);
            source += ALP_BLOCK_MAX_FLOAT_COUNT * sizeof(T);
            float_count -= ALP_BLOCK_MAX_FLOAT_COUNT;
        }

        if (float_count > 0)
            dest = encodeBlock(source, static_cast<UInt16>(float_count), dest);

        return static_cast<UInt32>(dest - dest_start);
    }

private:
    struct EncodingParams
    {
        UInt8 exponent;
        UInt8 fraction;
    };

    struct EncodingException
    {
        UInt16 index;
        T value;
    };

    struct BlockState
    {
        EncodingParams params;

        alignas(ALP_BUFFER_ALIGNMENT) Int64 encoded_floats[ALP_BLOCK_MAX_FLOAT_COUNT];
        UInt32 encoded_float_count;

        alignas(ALP_BUFFER_ALIGNMENT) UInt64 bitpacked[ALP_BLOCK_MAX_FLOAT_COUNT];
        UInt32 bitpacked_bytes;

        UInt8 bits;
        Int64 frame_of_reference;

        EncodingException exceptions[ALP_BLOCK_MAX_FLOAT_COUNT];
        UInt32 exceptions_count;
    };

    std::vector<EncodingParams> param_candidates;
    BlockState block;

    char * encodeBlock(const char * source, const UInt16 float_count, char * dest)
    {
        encodeBlockToState(source, float_count);

        // Check if encoding yields size reduction
        const size_t total_encoded_size = ALP_BLOCK_HEADER_SIZE + block.bitpacked_bytes + block.exceptions_count * (sizeof(UInt16) + sizeof(T));
        const size_t total_unencoded_size = ALP_UNENCODED_BLOCK_HEADER_SIZE + float_count * sizeof(T);
        if (total_encoded_size >= total_unencoded_size) // No compression gain
        {
            *dest++ = ALP_UNENCODED_BLOCK_EXPONENT; // Unencoded block marker
            return ALPUtils<T>::writeUnencoded(source, float_count, dest);
        }

        // Exponent and Fraction Indices
        *dest++ = block.params.exponent;
        *dest++ = block.params.fraction;

        // Exception Count
        unalignedStoreLittleEndian<UInt16>(dest, static_cast<UInt16>(block.exceptions_count));
        dest += sizeof(UInt16);

        // Encoding Bit-Width
        *dest++ = block.bits;

        // Frame of Reference Value
        unalignedStoreLittleEndian<Int64>(dest, block.frame_of_reference);
        dest += sizeof(Int64);

        // Write Encoded Values
        memcpy(dest, block.bitpacked, block.bitpacked_bytes);
        dest += block.bitpacked_bytes;

        // Write Exceptions
        for (UInt32 i = 0; i < block.exceptions_count; ++i)
        {
            const EncodingException & exception = block.exceptions[i];
            unalignedStoreLittleEndian<UInt16>(dest, exception.index);
            unalignedStoreLittleEndian<T>(dest + sizeof(UInt16), exception.value);
            dest += sizeof(UInt16) + sizeof(T);
        }

        return dest;
    }

    const char * encodeBlockToState(const char * source, const UInt16 float_count)
    {
        block.params = selectBlockParams(source, float_count);

        block.encoded_float_count = 0;
        block.exceptions_count = 0;

        Int64 min = std::numeric_limits<Int64>::max();
        Int64 max = std::numeric_limits<Int64>::min();

        for (UInt16 i = 0; i < float_count; ++i, source += sizeof(T))
        {
            const T value = unalignedLoadLittleEndian<T>(source);
            const Int64 value_enc = ALPFloatUtils<T>::encodeValue(value, block.params.exponent, block.params.fraction);
            const T value_dec = ALPFloatUtils<T>::decodeValue(value_enc, block.params.exponent, block.params.fraction);

            block.encoded_floats[block.encoded_float_count++] = value_enc;

            if (likely(value == value_dec))
            {
                min = std::min(value_enc, min);
                max = std::max(value_enc, max);
            }
            else
                block.exceptions[block.exceptions_count++] = {i, value};
        }

        block.frame_of_reference = min;
        block.bits = calculateEncodeBits(min, max);
        block.bitpacked_bytes = Compression::FFOR::calculateBitpackedBytes<ALP_BLOCK_MAX_FLOAT_COUNT>(block.bits);

        // Fill exceptions positions with frame_of_reference value, it becomes zero after FOR encoding
        for (UInt32 i = 0; i < block.exceptions_count; ++i)
            block.encoded_floats[block.exceptions[i].index] = block.frame_of_reference;

        // Fill remaining positions with min value (if any), FFOR always encodes 1024 values even if block is partial
        std::fill(block.encoded_floats + block.encoded_float_count, block.encoded_floats + ALP_BLOCK_MAX_FLOAT_COUNT, block.frame_of_reference);

        bitPackEncodedFloats();

        return source;
    }

    void bitPackEncodedFloats()
    {
        const UInt64 * __restrict in = reinterpret_cast<const UInt64 *>(block.encoded_floats);
        UInt64 * __restrict out = block.bitpacked;

        Compression::FFOR::bitPack<ALP_BLOCK_MAX_FLOAT_COUNT>(in, out, block.bits, block.frame_of_reference);
    }

    EncodingParams selectBlockParams(const char * source, const UInt32 float_count)
    {
        assert(param_candidates.size() > 0);
        if (param_candidates.size() == 1)
            return param_candidates[0];

        // Sample up to ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS values from the block for local parameter estimation.
        // Evenly select sample values across the block and copy them into a temporary buffer for evaluation.
        T sample[ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS];
        const UInt32 sample_count = std::min<UInt32>(float_count, ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS);
        const UInt32 sample_step = std::max(float_count / sample_count, 1u);
        for (UInt32 i = 0; i < sample_count; ++i)
            sample[i] = unalignedLoadLittleEndian<T>(&source[i * sample_step * sizeof(T)]);

        EncodingParams best_params = {0, 0};
        size_t best_size = std::numeric_limits<size_t>::max();
        bool is_prev_worse = false;

        for (const auto & params : param_candidates)
        {
            const size_t estimated_size = estimateEncodedSize(sample, sample_count, params);
            if (estimated_size < best_size)
            {
                best_size = estimated_size;
                best_params = params;
                is_prev_worse = false;
            }
            else if (estimated_size == best_size)
                is_prev_worse = false;
            else
            {
                if (is_prev_worse)
                    break; // Early stop if two consecutive candidates are worse
                is_prev_worse = true;
            }
        }

        return best_params;
    }

    void estimateParamCandidates(const char * source, const UInt32 float_count)
    {
        struct Estimation
        {
            EncodingParams params;
            UInt32 occurred_times;
        };
        std::unordered_map<UInt16, Estimation> estimations_map;

        // Take ALP_PARAMS_ESTIMATION_SAMPLES samples from the entire column for global parameter estimation.
        // Evenly select sample positions across the column.
        const UInt32 sample_step = float_count > ALP_PARAMS_ESTIMATION_SAMPLES * ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS
            ? (float_count - ALP_PARAMS_ESTIMATION_SAMPLES * ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS) / ALP_PARAMS_ESTIMATION_SAMPLES
            : ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS;

        T sample[ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS];

        // For each sample, brute-force over all valid (exponent, fraction) pairs to find the best parameters for that sample.
        for (UInt32 i = 0; i < ALP_PARAMS_ESTIMATION_SAMPLES; ++i)
        {
            const UInt32 sample_start_index = i * sample_step;
            if (sample_start_index >= float_count)
                break;

            const char * sample_pos = source + sample_start_index * sizeof(T);
            const UInt32 sample_float_count = std::min<UInt32>(ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS, float_count - sample_start_index);

            for (UInt32 j = 0; j < sample_float_count; ++j, sample_pos += sizeof(T))
                sample[j] = unalignedLoadLittleEndian<T>(sample_pos);

            Estimation best_estimation = {{0, 0}, 0};
            size_t best_size = std::numeric_limits<size_t>::max();

            for (UInt8 exponent = 0; exponent < ALPFloatTraits<T>::EXPONENT_COUNT; ++exponent)
            {
                for (UInt8 fraction = 0; fraction <= exponent; ++fraction)
                {
                    const Estimation estimation{{exponent, fraction}, 1};
                    const size_t estimated_size = estimateEncodedSize(sample, sample_float_count, estimation.params);

                    if (estimated_size < best_size)
                    {
                        best_size = estimated_size;
                        best_estimation = estimation;
                    }
                }
            }

            const UInt16 key = static_cast<UInt16>((best_estimation.params.exponent << 8) | best_estimation.params.fraction);
            auto it = estimations_map.find(key);
            if (it != estimations_map.end())
                ++(it->second.occurred_times);
            else
                estimations_map[key] = best_estimation;
        }

        // Sort estimations by occurred times desc, exponent asc, fraction asc
        std::array<Estimation, ALP_PARAMS_ESTIMATION_SAMPLES> estimations;
        UInt32 estimation_count = 0;
        for (const auto & [_, estimation] : estimations_map)
            estimations[estimation_count++] = estimation;
        std::sort(estimations.begin(), estimations.begin() + estimation_count,
            [](const Estimation & a, const Estimation & b)
            {
                if (a.occurred_times == b.occurred_times)
                {
                    if (a.params.exponent == b.params.exponent)
                        return a.params.fraction < b.params.fraction;
                    return a.params.exponent < b.params.exponent;
                }
                return a.occurred_times > b.occurred_times;
            });

        // Keep top ALP_PARAMS_ESTIMATION_CANDIDATES candidates
        param_candidates.clear();
        estimation_count = std::min(estimation_count, ALP_PARAMS_ESTIMATION_CANDIDATES);
        for (UInt32 i = 0; i < estimation_count; ++i)
            param_candidates.push_back(estimations[i].params);
    }

    static size_t estimateEncodedSize(const T * const source, const UInt32 float_count, const EncodingParams & params)
    {
        Int64 min = std::numeric_limits<Int64>::max();
        Int64 max = std::numeric_limits<Int64>::min();
        UInt32 exception_count = 0;

        for (UInt32 i = 0; i < float_count; ++i)
        {
            const T value = source[i];
            const Int64 value_enc = ALPFloatUtils<T>::encodeValue(value, params.exponent, params.fraction);
            const T value_dec = ALPFloatUtils<T>::decodeValue(value_enc, params.exponent, params.fraction);

            if (likely(value == value_dec))
            {
                min = std::min(value_enc, min);
                max = std::max(value_enc, max);
            }
            else
                ++exception_count;
        }

        const UInt8 bits = calculateEncodeBits(min, max);
        const size_t total_size = ALP_BLOCK_HEADER_SIZE + float_count * bits / 8 + exception_count * (sizeof(UInt16) + sizeof(T));
        return total_size;
    }

    static UInt8 calculateEncodeBits(const Int64 min_value, const Int64 max_value)
    {
        if (unlikely(min_value > max_value))
            return sizeof(Int64) * 8; // Edge case when no values are encoded or overflow happened.

        // Cast to UInt64 to correctly handle the full Int64 range and potential overflow when values are close to the limits.
        const UInt64 diff = static_cast<UInt64>(max_value) - static_cast<UInt64>(min_value);

        const UInt8 bits = ALPUtils<T>::calculateBitWidth(diff);
        return bits;
    }
};

template <FLOAT T>
class ALPCodecDecoder
{
public:
    explicit ALPCodecDecoder() = default;

    UInt32 decode(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
    {
        if (uncompressed_size % sizeof(T) != 0)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP-encoded data, invalid uncompressed size");

        const char * source_end = source + source_size;
        const char * dest_start = dest;
        const char * dest_end = dest + uncompressed_size;

        while (source < source_end)
        {
            const UInt16 block_float_count = static_cast<UInt16>(std::min<size_t>(ALP_BLOCK_MAX_FLOAT_COUNT, (dest_end - dest) / sizeof(T)));
            decodeBlock(source, source_end, dest, dest_end, block_float_count);
        }

        if (source != source_end || dest != dest_end)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP-encoded data, stream size mismatch");

        return static_cast<UInt32>(dest - dest_start);
    }

private:
    struct BlockState
    {
        alignas(ALP_BUFFER_ALIGNMENT) Int64 encoded[ALP_BLOCK_MAX_FLOAT_COUNT];
        alignas(ALP_BUFFER_ALIGNMENT) UInt64 bitpacked[ALP_BLOCK_MAX_FLOAT_COUNT];
    };

    BlockState block;

    void decodeBlock(const char * & source, const char * source_end, char * & dest, const char * dest_end, const UInt16 float_count)
    {
        if (unlikely(source + ALP_UNENCODED_BLOCK_HEADER_SIZE > source_end))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress ALP-encoded data, incomplete block header");

        // Read exponent byte and check for unencoded block marker
        const UInt8 exponent = static_cast<UInt8>(*source++);
        if (exponent == ALP_UNENCODED_BLOCK_EXPONENT)
        {
            ALPUtils<T>::decompressUnencodedBlock(source, source_end, dest, dest_end, float_count);
            return;
        }
        if (unlikely(exponent >= ALPFloatTraits<T>::EXPONENT_COUNT))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress ALP-encoded data, invalid exponent value: {}, max allowed: {}",
                static_cast<UInt32>(exponent), static_cast<UInt32>(ALPFloatTraits<T>::EXPONENT_COUNT - 1));

        if (unlikely(source + (ALP_BLOCK_HEADER_SIZE - ALP_UNENCODED_BLOCK_HEADER_SIZE) > source_end))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress ALP-encoded data, incomplete block header (encoded)");

        // Read fraction
        const UInt8 fraction = static_cast<UInt8>(*source++);
        if (unlikely(fraction >= ALPFloatTraits<T>::EXPONENT_COUNT))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress ALP-encoded data, invalid fraction value: {}, max allowed: {}",
                static_cast<UInt32>(fraction), static_cast<UInt32>(ALPFloatTraits<T>::EXPONENT_COUNT - 1));

        // Read exception count
        const UInt16 exception_count = unalignedLoadLittleEndian<UInt16>(source);
        source += sizeof(UInt16);

        // Read bits
        const UInt8 bits = static_cast<UInt8>(*source++);
        const UInt32 bitpacked_size = Compression::FFOR::calculateBitpackedBytes<ALP_BLOCK_MAX_FLOAT_COUNT>(bits);

        // Read frame of reference
        const Int64 frame_of_reference = unalignedLoadLittleEndian<Int64>(source);
        source += sizeof(Int64);

        // Validate block payload size
        const size_t total_encoded_size = bitpacked_size + exception_count * (sizeof(UInt16) + sizeof(T));
        if (unlikely(source + total_encoded_size > source_end))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress ALP-encoded data, incomplete block payload, available size: {}, bit-width: {}, exceptions: {}",
                source_end - source, static_cast<UInt32>(bits), static_cast<UInt32>(exception_count));

        // Read bit-packed values into temporary buffer and decode
        memcpy(block.bitpacked, source, bitpacked_size);
        source += bitpacked_size;
        bitUnpackEncodedFloats(bits, frame_of_reference);

        // Write decoded values to output buffer
        char * dest_start = dest;
        for (UInt16 i = 0; i < float_count; ++i, dest += sizeof(T))
        {
            const T decoded_value = ALPFloatUtils<T>::decodeValue(block.encoded[i], exponent, fraction);
            unalignedStoreLittleEndian<T>(dest, decoded_value);
        }

        // Write exceptions into corresponding positions in output buffer
        for (UInt16 i = 0; i < exception_count; ++i)
        {
            const UInt16 exception_index = unalignedLoadLittleEndian<UInt16>(source);
            const T exception_value = unalignedLoadLittleEndian<T>(source + sizeof(UInt16));
            source += sizeof(UInt16) + sizeof(T);

            if (unlikely(exception_index >= float_count))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                    "Cannot decompress ALP-encoded data, invalid exception index, index: {}, float count: {}",
                    exception_index, float_count);

            const UInt32 dest_offset = exception_index * sizeof(T);
            unalignedStoreLittleEndian<T>(dest_start + dest_offset, exception_value);
        }
    }

    void bitUnpackEncodedFloats(const UInt8 bits, const Int64 frame_of_reference)
    {
        const UInt64 * __restrict in = block.bitpacked;
        UInt64 * __restrict out = reinterpret_cast<UInt64 *>(block.encoded);

        Compression::FFOR::bitUnpack<ALP_BLOCK_MAX_FLOAT_COUNT>(in, out, bits, frame_of_reference);
    }
};

template <FLOAT T>
class ALPRDCodecEncoder
{
public:
    ALPRDCodecEncoder() = default;

    UInt32 encode(const char * source, const UInt32 source_size, char * dest)
    {
        if (source_size % sizeof(T) != 0)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with ALP(RD) codec, data size {} is not aligned to {}", source_size, sizeof(T));

        UInt32 float_count = source_size / sizeof(T);

        const char * dest_start = dest;

        while (float_count >= ALP_BLOCK_MAX_FLOAT_COUNT)
        {
            dest = encodeBlock(source, ALP_BLOCK_MAX_FLOAT_COUNT, dest);
            source += ALP_BLOCK_MAX_FLOAT_COUNT * sizeof(T);
            float_count -= ALP_BLOCK_MAX_FLOAT_COUNT;
        }

        if (float_count > 0)
            dest = encodeBlock(source, static_cast<UInt16>(float_count), dest);

        return static_cast<UInt32>(dest - dest_start);
    }

private:
    using Unsigned = std::conditional_t<std::is_same_v<T, Float32>, UInt32, UInt64>;

    struct DictParams
    {
        UInt8 left_bits;
        UInt8 right_bits;

        std::vector<std::pair<UInt32, UInt16>> left_part_freq; // frequency desc, left part value asc

        size_t estimated_size;
    };

    struct EncodingException
    {
        UInt16 index;
        T value;
    };

    struct BlockState
    {
        UInt8 left_bits;

        std::unordered_map<UInt16, UInt8> dict_map; // left part value → dictionary index
        std::vector<UInt16> dict_values; // left part value for each dictionary index, sorted by frequency desc (index 0 is most frequent)

        UInt8 right_bits;

        alignas(ALP_BUFFER_ALIGNMENT) UInt16 encoded_left[ALP_BLOCK_MAX_FLOAT_COUNT];
        alignas(ALP_BUFFER_ALIGNMENT) UInt16 bitpacked_left[ALP_BLOCK_MAX_FLOAT_COUNT];
        UInt16 bitpacked_left_bytes;

        alignas(ALP_BUFFER_ALIGNMENT) Unsigned encoded_right[ALP_BLOCK_MAX_FLOAT_COUNT];
        alignas(ALP_BUFFER_ALIGNMENT) Unsigned bitpacked_right[ALP_BLOCK_MAX_FLOAT_COUNT];
        UInt16 bitpacked_right_bytes;

        EncodingException exceptions[ALP_BLOCK_MAX_FLOAT_COUNT];
        UInt32 exceptions_count;
    };

    BlockState block;

    char * encodeBlock(const char * source, const UInt16 float_count, [[maybe_unused]] char * dest)
    {
        encodeBlockToState(source, float_count);

        // Check if encoding yields size reduction
        const UInt8 dict_bits = ALPUtils<T>::calculateBitWidth(block.dict_values.size());
        const size_t total_encoded_size = ALP_RD_BLOCK_HEADER_SIZE +
            block.dict_values.size() * sizeof(UInt16) + // Dictionary values
                Compression::FFOR::calculateBitpackedBytes(dict_bits) + // Left part bit-packed indices
                    Compression::FFOR::calculateBitpackedBytes(block.right_bits) + // Right part bit-packed values
                        block.exceptions_count * (sizeof(UInt16) + sizeof(T)); // Exceptions
        const size_t total_unencoded_size = ALP_UNENCODED_BLOCK_HEADER_SIZE + float_count * sizeof(T);
        if (total_encoded_size >= total_unencoded_size) // No compression gain
        {
            *dest++ = ALP_RD_UNENCODED_BLOCK_BIT_WIDTH; // Unencoded block marker
            return ALPUtils<T>::writeUnencoded(source, float_count, dest);
        }

        // Left Bit-Width
        *dest++ = block.left_bits;

        // Dictionary Size
        *dest++ = static_cast<UInt8>(block.dict_values.size());

        // Exception Count
        unalignedStoreLittleEndian<UInt16>(dest, static_cast<UInt16>(block.exceptions_count));
        dest += sizeof(UInt16);

        // Write Dictionary Values
        for (const UInt16 left_part : block.dict_values)
        {
            unalignedStoreLittleEndian<UInt16>(dest, left_part);
            dest += sizeof(UInt16);
        }

        // Write Left Part Bit-Packed Indices
        memcpy(dest, block.bitpacked_left, block.bitpacked_left_bytes);
        dest += block.bitpacked_left_bytes;

        // Write Right Part Bit-Packed Values
        memcpy(dest, block.bitpacked_right, block.bitpacked_right_bytes);
        dest += block.bitpacked_right_bytes;

        // Write Exceptions
        for (UInt32 i = 0; i < block.exceptions_count; ++i)
        {
            const EncodingException & exception = block.exceptions[i];
            unalignedStoreLittleEndian<UInt16>(dest, exception.index);
            unalignedStoreLittleEndian<T>(dest + sizeof(UInt16), exception.value);
            dest += sizeof(UInt16) + sizeof(T);
        }

        return dest;
    }

    void encodeBlockToState(const char * source, const UInt16 float_count)
    {
        estimateBlockDictParams(source, float_count);

        Unsigned right_part_mask = (static_cast<Unsigned>(1) << block.right_bits) - 1;

        block.exceptions_count = 0;

        for (UInt16 i = 0; i < float_count; ++i, source += sizeof(T))
        {
            const Unsigned value = unalignedLoadLittleEndian<Unsigned>(source); // reinterpret float bits as unsigned integer

            const UInt16 left_part = static_cast<UInt16>(value >> block.right_bits);
            const auto dict_it = block.dict_map.find(left_part);
            if (dict_it != block.dict_map.end())
                block.encoded_left[i] = dict_it->second;
            else
            {
                block.encoded_left[i] = 0;
                block.exceptions[block.exceptions_count++] = {i, std::bit_cast<T>(value)};
            }

            block.encoded_right[i] = static_cast<Unsigned>(value & right_part_mask);

            /*std::cout << "Value: " << std::bitset<sizeof(Unsigned) * 8>(value)
                << ", Left Part: " << std::bitset<sizeof(UInt16) * 8>(left_part)
                << ", Right Part: " << std::bitset<sizeof(Unsigned) * 8>(block.encoded_right[i])
                << ", Dict Index: " << static_cast<UInt32>(block.encoded_left[i])
                << (dict_it != block.dict_map.end() ? "" : " (Exception)") << std::endl;*/
        }

        auto dict_bits = ALPUtils<T>::calculateBitWidth(block.dict_values.size());
        block.bitpacked_left_bytes = Compression::FFOR::calculateBitpackedBytes(dict_bits);
        Compression::FFOR::bitPack(block.encoded_left, block.bitpacked_left, dict_bits, 0);

        block.bitpacked_right_bytes = Compression::FFOR::calculateBitpackedBytes(block.right_bits);
        Compression::FFOR::bitPack(block.encoded_right, block.bitpacked_right, block.right_bits, 0);
    }

    void estimateBlockDictParams(const char * source, const UInt16 float_count)
    {
        // Sample up to ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS values from the block for local parameter estimation.
        // Evenly select sample values across the block and copy them into a temporary buffer for evaluation.
        T sample[ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS];
        const UInt16 sample_count = std::min<UInt16>(float_count, ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS);
        const UInt16 sample_step = std::max<UInt16>(float_count / sample_count, 1u);
        for (UInt16 i = 0; i < sample_count; ++i)
            sample[i] = unalignedLoadLittleEndian<T>(&source[i * sample_step * sizeof(T)]);

        DictParams best_params
        {
            .left_bits = 0,
            .right_bits = static_cast<UInt8>(sizeof(Unsigned) * 8),
            .left_part_freq = {},
            .estimated_size = std::numeric_limits<size_t>::max()
        };
        bool is_prev_worse = false;

        for (UInt8 left_bits = 1; left_bits <= ALP_RD_CUTTING_LIMIT; ++left_bits)
        {
            auto params = estimateBlockDictParamsCandidate(sample, sample_count, left_bits);
            if (params.estimated_size < best_params.estimated_size)
            {
                best_params = params;
                is_prev_worse = false;
            }
            else if (params.estimated_size == best_params.estimated_size)
                is_prev_worse = false;
            else
            {
                if (is_prev_worse)
                    break; // Early stop if two consecutive candidates are worse
                is_prev_worse = true;
            }
        }

        block.left_bits = best_params.left_bits;
        block.right_bits = best_params.right_bits;

        block.dict_map.clear();
        block.dict_map.reserve(best_params.left_part_freq.size());

        block.dict_values.clear();
        block.dict_values.reserve(best_params.left_part_freq.size());

        for (size_t i = 0; i < best_params.left_part_freq.size(); ++i)
        {
            const UInt16 left_part = best_params.left_part_freq[i].second;
            block.dict_values.push_back(left_part);
            block.dict_map[left_part] = static_cast<UInt8>(i);
        }
    }

    static DictParams estimateBlockDictParamsCandidate(const T * const source, const UInt16 float_count, const UInt8 left_bits)
    {
        DictParams params
        {
            .left_bits = left_bits,
            .right_bits = static_cast<UInt8>(sizeof(Unsigned) * 8 - left_bits),
            .left_part_freq = {},
            .estimated_size = std::numeric_limits<size_t>::max()
        };

        // Count frequency of left part values
        std::unordered_map<UInt16, UInt32> left_part_freq(float_count); // left part value → occurrence count
        auto * ut_source = reinterpret_cast<const Unsigned *>(source);
        for (UInt16 i = 0; i < float_count; ++i)
        {
            auto left_part = static_cast<UInt16>(ut_source[i] >> params.right_bits);
            ++left_part_freq[left_part];
        }

        // Sort left part values by frequency desc, left part value asc
        params.left_part_freq.reserve(left_part_freq.size());
        for (auto & [f, s] : left_part_freq)
            params.left_part_freq.emplace_back(s, f);
        std::sort(params.left_part_freq.begin(), params.left_part_freq.end(),
                  [](const auto & a, const auto & b) {
                      // Sort by frequency desc, left part value asc
                      return a.first != b.first ? a.first > b.first : a.second < b.second;
                  });

        // Number of values that cannot be encoded with the current left_bits and would become exceptions
        UInt32 exceptions_count = 0;
        for (UInt32 i = ALP_RD_MAX_DICT_SIZE; i < params.left_part_freq.size(); ++i)
            exceptions_count += params.left_part_freq[i].first;

        // Trim dictionary to max size and calculate estimated encoding size
        if (params.left_part_freq.size() > ALP_RD_MAX_DICT_SIZE)
            params.left_part_freq.resize(ALP_RD_MAX_DICT_SIZE);

        // Estimated size
        auto dict_size = params.left_part_freq.size() * sizeof(UInt16);
        auto dict_bits = ALPUtils<T>::calculateBitWidth(params.left_part_freq.size());
        auto encoded_left_size = Compression::FFOR::calculateBitpackedBytes<ALP_BLOCK_MAX_FLOAT_COUNT>(dict_bits);
        auto encoded_right_size = Compression::FFOR::calculateBitpackedBytes<ALP_BLOCK_MAX_FLOAT_COUNT>(params.right_bits);
        auto exceptions_size = exceptions_count * (sizeof(UInt16) + sizeof(T));

        params.estimated_size = ALP_RD_BLOCK_HEADER_SIZE + dict_size + encoded_left_size + encoded_right_size + exceptions_size;

        return params;
    }
};

template <FLOAT T>
class ALPRDCodecDecoder
{
public:
    explicit ALPRDCodecDecoder() = default;

    UInt32 decode(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
    {
        if (uncompressed_size % sizeof(T) != 0)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP(RD)-encoded data, invalid uncompressed size");

        const char * source_end = source + source_size;
        const char * dest_start = dest;
        const char * dest_end = dest + uncompressed_size;

        while (source < source_end)
        {
            const UInt16 block_float_count = static_cast<UInt16>(std::min<size_t>(ALP_BLOCK_MAX_FLOAT_COUNT, (dest_end - dest) / sizeof(T)));
            decodeBlock(source, source_end, dest, dest_end, block_float_count);
        }

        if (source != source_end || dest != dest_end)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP(RD)-encoded data, stream size mismatch");

        return static_cast<UInt32>(dest - dest_start);
    }

private:
    using Unsigned = std::conditional_t<std::is_same_v<T, Float32>, UInt32, UInt64>;

    struct BlockState
    {
        std::vector<UInt16> dict_values;

        alignas(ALP_BUFFER_ALIGNMENT) UInt16 encoded_left[ALP_BLOCK_MAX_FLOAT_COUNT];
        alignas(ALP_BUFFER_ALIGNMENT) UInt16 bitpacked_left[ALP_BLOCK_MAX_FLOAT_COUNT];

        alignas(ALP_BUFFER_ALIGNMENT) Unsigned encoded_right[ALP_BLOCK_MAX_FLOAT_COUNT];
        alignas(ALP_BUFFER_ALIGNMENT) Unsigned bitpacked_right[ALP_BLOCK_MAX_FLOAT_COUNT];
    };

    BlockState block;

    void decodeBlock(const char * & source, const char * source_end, char * & dest, const char * dest_end, const UInt16 float_count)
    {
        if (unlikely(source + ALP_RD_UNENCODED_BLOCK_HEADER_SIZE > source_end))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress ALP(RD)-encoded data, incomplete block header");

        // Read left bit-width and check for unencoded block marker
        const UInt8 left_bits = static_cast<UInt8>(*source++);
        if (left_bits == ALP_RD_UNENCODED_BLOCK_BIT_WIDTH)
        {
            ALPUtils<T>::decompressUnencodedBlock(source, source_end, dest, dest_end, float_count);
            return;
        }
        if (unlikely(left_bits > ALP_RD_CUTTING_LIMIT))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress ALP(RD)-encoded data, invalid left bit-width: {}, max allowed: {}",
                static_cast<UInt32>(left_bits), static_cast<UInt32>(ALP_RD_CUTTING_LIMIT));

        if (unlikely(source + (ALP_RD_BLOCK_HEADER_SIZE - ALP_RD_UNENCODED_BLOCK_HEADER_SIZE) > source_end))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress ALP(RD)-encoded data, incomplete block header (encoded)");

        // Read dictionary size
        const UInt8 dict_size = static_cast<UInt8>(*source++);
        if (unlikely(dict_size > ALP_RD_MAX_DICT_SIZE))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress ALP(RD)-encoded data, invalid dictionary size: {}, max allowed: {}",
                static_cast<UInt32>(dict_size), static_cast<UInt32>(ALP_RD_MAX_DICT_SIZE));

        // Read exception count
        const UInt16 exception_count = unalignedLoadLittleEndian<UInt16>(source);
        source += sizeof(UInt16);

        // Validate block payload size
        UInt8 dict_bits = ALPUtils<T>::calculateBitWidth(dict_size);
        UInt16 bitpacked_left_bytes = Compression::FFOR::calculateBitpackedBytes<ALP_BLOCK_MAX_FLOAT_COUNT>(dict_bits);
        UInt8 right_bits = static_cast<UInt8>(sizeof(Unsigned) * 8 - left_bits);
        UInt16 bitpacked_right_bytes = Compression::FFOR::calculateBitpackedBytes<ALP_BLOCK_MAX_FLOAT_COUNT>(right_bits);
        const size_t total_encoded_size =
            dict_size * sizeof(UInt16) + // Dictionary values
                bitpacked_left_bytes + // Left part bit-packed indices
                    bitpacked_right_bytes + // Right part bit-packed values
                        exception_count * (sizeof(UInt16) + sizeof(T)); // Exceptions
        if (unlikely(source + total_encoded_size > source_end))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress ALP(RD)-encoded data, incomplete block payload, available size: {}, left bit-width: {}, dictionary size: {}, exceptions: {}",
                source_end - source, static_cast<UInt32>(left_bits), static_cast<UInt32>(dict_size), static_cast<UInt32>(exception_count));

        // Read dictionary values
        block.dict_values.clear();
        block.dict_values.reserve(dict_size);
        for (UInt16 i = 0; i < dict_size; ++i)
        {
            const UInt16 dict_entry = unalignedLoadLittleEndian<UInt16>(source);
            source += sizeof(UInt16);
            block.dict_values.push_back(dict_entry);
        }

        // Read left part bit-packed indices into temporary buffer and decode
        memcpy(block.bitpacked_left, source, bitpacked_left_bytes);
        source += bitpacked_left_bytes;
        Compression::FFOR::bitUnpack(block.bitpacked_left, block.encoded_left, dict_bits, 0);

        // Read right part bit-packed values into temporary buffer and decode
        memcpy(block.bitpacked_right, source, bitpacked_right_bytes);
        source += bitpacked_right_bytes;
        Compression::FFOR::bitUnpack(block.bitpacked_right, block.encoded_right, right_bits, 0);

        // Write decoded values to output buffer
        char * dest_start = dest;
        for (UInt16 i = 0; i < float_count; ++i, dest += sizeof(T))
        {
            const UInt16 dict_index = block.encoded_left[i];
            if (unlikely(dict_index >= dict_size))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                    "Cannot decompress ALP(RD)-encoded data, invalid dictionary index: {}, dict size: {}",
                    dict_index, static_cast<UInt16>(dict_size));

            const UInt16 left_part = block.dict_values[dict_index];
            const Unsigned right_part = block.encoded_right[i];
            const Unsigned decoded_value = (static_cast<Unsigned>(left_part) << right_bits) | right_part;

            unalignedStoreLittleEndian<Unsigned>(dest, decoded_value);
        }

        // Write exceptions into corresponding positions in output buffer
        for (UInt16 i = 0; i < exception_count; ++i)
        {
            const UInt16 exception_index = unalignedLoadLittleEndian<UInt16>(source);
            const T exception_value = unalignedLoadLittleEndian<T>(source + sizeof(UInt16));
            source += sizeof(UInt16) + sizeof(T);

            if (unlikely(exception_index >= float_count))
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                    "Cannot decompress ALP-encoded data, invalid exception index, index: {}, float count: {}",
                    exception_index, float_count);

            const UInt32 dest_offset = exception_index * sizeof(T);
            unalignedStoreLittleEndian<T>(dest_start + dest_offset, exception_value);
        }
    }
};

}

CompressionCodecALP::CompressionCodecALP(UInt8 float_width_, Variant variant_)
    : float_width(float_width_), variant(variant_)
{
    const String variant_str = variant_ == Variant::STD ? "STD" : "RD";
    setCodecDescription("ALP", {make_intrusive<ASTIdentifier>(variant_str)});
}

uint8_t CompressionCodecALP::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::ALP);
}

void CompressionCodecALP::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /* ignore_aliases */ true);
}

String CompressionCodecALP::getDescription() const
{
    return "Adaptive Lossless floating-Point; suitable for time series data.";
}

UInt32 CompressionCodecALP::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // Maximum possible encoding size = uncompressed data + codec header + number of blocks * block header
    const UInt32 num_blocks = uncompressed_size / float_width / ALP_BLOCK_MAX_FLOAT_COUNT + 1;
    return uncompressed_size + ALP_CODEC_HEADER_SIZE + num_blocks * ALP_BLOCK_HEADER_SIZE;
}

UInt32 CompressionCodecALP::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    // Write ALP header
    *dest++ = ALP_CODEC_VERSION | (variant == Variant::RD ? 1 << 4 : 0); // meta_byte (version and variant)
    *dest++ = float_width;
    unalignedStoreLittleEndian<UInt16>(dest, ALP_BLOCK_MAX_FLOAT_COUNT);
    dest += sizeof(UInt16);

    UInt32 dest_size = 0;

    if (float_width == sizeof(Float32))
    {
        if (variant == Variant::STD)
            dest_size = ALPCodecEncoder<Float32>().encode(source, source_size, dest);
        else if (variant == Variant::RD)
            dest_size = ALPRDCodecEncoder<Float32>().encode(source, source_size, dest);
        else
            throw Exception(ErrorCodes::CANNOT_COMPRESS,
                "Cannot compress with codec ALP, unsupported variant {}",
                variant);
    }
    else if (float_width == sizeof(Float64))
    {
        if (variant == Variant::STD)
            dest_size = ALPCodecEncoder<Float64>().encode(source, source_size, dest);
        else if (variant == Variant::RD)
            dest_size = ALPRDCodecEncoder<Float64>().encode(source, source_size, dest);
        else
            throw Exception(ErrorCodes::CANNOT_COMPRESS,
                "Cannot compress with codec ALP, unsupported variant {}",
                variant);
    }
    else
        throw Exception(ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with codec ALP, unsupported float width {}",
            static_cast<size_t>(float_width));

    return dest_size + ALP_CODEC_HEADER_SIZE;
}

UInt32 CompressionCodecALP::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < ALP_CODEC_HEADER_SIZE)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP-encoded data, data has wrong header");

    const UInt8 meta_byte = static_cast<UInt8>(*source++);
    const UInt8 codec_version = meta_byte & 0x0F;
    const UInt8 codec_variant = meta_byte >> 4 & 0x01;
    const bool codec_std_variant = codec_variant == 0;

    if (codec_version != ALP_CODEC_VERSION)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ALP-encoded data, unsupported codec version {}",
            static_cast<UInt32>(codec_version));

    const UInt8 data_float_width = static_cast<UInt8>(*source++);

    const UInt16 block_float_count = unalignedLoadLittleEndian<UInt16>(source);
    source += sizeof(UInt16);
    if (block_float_count != ALP_BLOCK_MAX_FLOAT_COUNT)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ALP-encoded data, supported block float count is {}, got {}",
            ALP_BLOCK_MAX_FLOAT_COUNT, block_float_count);

    source_size -= ALP_CODEC_HEADER_SIZE;
    UInt32 dest_size;

    if (data_float_width == sizeof(Float32))
    {
        if (codec_std_variant)
            dest_size = ALPCodecDecoder<Float32>().decode(source, source_size, dest, uncompressed_size);
        else
            dest_size = ALPRDCodecDecoder<Float32>().decode(source, source_size, dest, uncompressed_size);
    }
    else if (data_float_width == sizeof(Float64))
    {
        if (codec_std_variant)
            dest_size = ALPCodecDecoder<Float64>().decode(source, source_size, dest, uncompressed_size);
        else
            dest_size = ALPRDCodecDecoder<Float64>().decode(source, source_size, dest, uncompressed_size);
    }
    else
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ALP-encoded data, unsupported float width {}",
            static_cast<size_t>(data_float_width));

    return dest_size;
}

void registerCodecALP(CompressionCodecFactory & factory)
{
    auto method_code = static_cast<UInt8>(CompressionMethodByte::ALP);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        if (arguments && arguments->children.size() > 1)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                "ALP codec must have zero or one parameter, given {}",
                arguments->children.size());

        UInt8 float_width = sizeof(Float64);
        if (column_type)
        {
            if (!WhichDataType(column_type).isNativeFloat())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Codec ALP is not applicable for {} because the data type is not Float*",
                    column_type->getName());

            float_width = static_cast<UInt8>(column_type->getSizeOfValueInMemory());
        }

        CompressionCodecALP::Variant variant = CompressionCodecALP::Variant::STD;
        if (arguments && arguments->children.size() == 1)
        {
            const auto * variant_ident = arguments->children[0]->as<ASTIdentifier>();
            if (!variant_ident)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                    "ALP codec variant must be an identifier with STD or RD");

            const String variant_str = variant_ident->shortName();
            if (variant_str == "STD")
                variant = CompressionCodecALP::Variant::STD;
            else if (variant_str == "RD")
                variant = CompressionCodecALP::Variant::RD;
            else
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                    "ALP codec variant must be STD or RD, given {}",
                    variant_str);
        }

        return std::make_shared<CompressionCodecALP>(float_width, variant);
    };
    factory.registerCompressionCodecWithType("ALP", method_code, codec_builder);
}
}
