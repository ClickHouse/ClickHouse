#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>

#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <Parsers/IAST.h>
#include <base/unaligned.h>

#include <Compression/FFOR.h>

#include <array>

namespace DB
{
/**
 * ALP (Adaptive Lossless floating-Point) compression codec.
 *
 * This implementation is based on the ALP paper (https://ir.cwi.nl/pub/33334) and implements the main ALP variant for Float32 and Float64.
 * It encodes floating-point values as scaled integers (plus a small set of exceptions), then applies Frame-of-Reference + bit-packing.
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
 *     - bit 4:    variant flag (0 = ALP, 1 = ALP_RD; only 0 supported)
 *     - bits 5-7: reserved (must be 0)
 *   - float width (1 byte):
 *     - 4 or 8 bytes (Float32 / Float64)
 *   - block float count (2 bytes):
 *     - number of floats per block (UInt16), currently fixed to 1024, other values are not supported.
 *
 * The input column is split into blocks of up to ALP_BLOCK_MAX_FLOAT_COUNT values (1024).
 * Each block is encoded independently and can be either compressed or left raw, depending on the estimated gain.
 *
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
 * Notes:
 *   - This codec implements the ALP variant only.
 *   - ALP_RD (front-bits-based encoder for “real doubles”) is not implemented.
 *   - Supported types: 4 and 8 bytes floating point.
 *   - The scheme is fully lossless: all non-exception values are proven round-trip encodable; all others are stored as raw exceptions.
 */
class CompressionCodecALP final : public ICompressionCodec
{
public:
    explicit CompressionCodecALP(UInt8 float_width_);
    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;
protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;
    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    bool isFloatingPointTimeSeriesCodec() const override { return true; }
    String getDescription() const override;
private:
    UInt8 float_width;
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

template<>
struct ALPFloatTraits<Float64>
{
    /**
     * Float64 scaling is limited to 10^17 and inputs are clamped to ±922337203685477478.
     * In this range, a meaningful subset of values still "survives" scale by 10^e → round → cast to int64.
     * Around ~1e18 Float64 becomes too sparse, so after decimal scaling most values no longer map stably to an integer and the result differ between x86 and ARM.
     * The reference implementation use ~10x wider bounds, which led to cross-arch divergence encoded output.
     * These conservative limits keep encoding bit-identical across platforms.
     */
    static constexpr UInt8 EXPONENT_COUNT = 18;

    static constexpr std::array<Float64, EXPONENT_COUNT> EXPONENTS = generatePowersOf10<Float64, EXPONENT_COUNT, false>();
    static constexpr std::array<Float64, EXPONENT_COUNT> FRACTIONS = generatePowersOf10<Float64, EXPONENT_COUNT, true>();

    static constexpr Float64 UPPER = 922337203685477478.0;
    static constexpr Float64 LOWER = -922337203685477478.0;

    static constexpr Float64 ROUND_MAGIC = 6755399441055744.0; // 2^51 + 2^52
};

template<>
struct ALPFloatTraits<Float32>
{
    static constexpr UInt8 EXPONENT_COUNT = 10;

    static constexpr std::array<Float32, EXPONENT_COUNT> EXPONENTS = generatePowersOf10<Float32, EXPONENT_COUNT, false>();
    static constexpr std::array<Float32, EXPONENT_COUNT> FRACTIONS = generatePowersOf10<Float32, EXPONENT_COUNT, true>();

    static constexpr Float32 UPPER = 922337203685477478.0f;
    static constexpr Float32 LOWER = -922337203685477478.0f;

    static constexpr Float32 ROUND_MAGIC = 12582912.0; // 2^22 + 2^23
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
            return writeUnencoded(source, float_count, dest);

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

    static char * writeUnencoded(const char * source, const UInt16 float_count, char * dest)
    {
        *dest++ = ALP_UNENCODED_BLOCK_EXPONENT; // Unencoded block marker

        const size_t block_size = float_count * sizeof(T);
        memcpy(dest, source, block_size);
        dest += block_size;

        return dest;
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

        const UInt64 diff = static_cast<UInt64>(max_value - min_value);
        if (unlikely(diff == 0))
            return 0; // Edge case when all values are the same.

        const auto bits = sizeof(Int64) * 8 - getLeadingZeroBitsUnsafe<UInt64>(diff);
        return static_cast<UInt8>(bits);
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
            processUnencodedBlock(source, source_end, dest, dest_end, float_count);
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

    static void processUnencodedBlock(const char * & source, const char * source_end, char * & dest, const char * dest_end, const UInt16 float_count)
    {
        const size_t block_size = float_count * sizeof(T);
        if (source + block_size > source_end || dest + block_size > dest_end)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP-encoded data, incomplete uncompressed block");

        memcpy(dest, source, block_size);
        source += block_size;
        dest += block_size;
    }
};

}

CompressionCodecALP::CompressionCodecALP(UInt8 float_width_)
    : float_width(float_width_)
{
    setCodecDescription("ALP", {});
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
    *dest++ = ALP_CODEC_VERSION; // meta_byte: version = 1, variant = 0
    *dest++ = float_width;
    unalignedStoreLittleEndian<UInt16>(dest, ALP_BLOCK_MAX_FLOAT_COUNT);
    dest += sizeof(UInt16);

    UInt32 dest_size = 0;

    switch (float_width)
    {
    case sizeof(Float32):
        dest_size = ALPCodecEncoder<Float32>().encode(source, source_size, dest);
        break;
    case sizeof(Float64):
        dest_size = ALPCodecEncoder<Float64>().encode(source, source_size, dest);
        break;
    default:
        throw Exception(ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with codec ALP, unsupported float width {}",
            static_cast<size_t>(float_width));
    }

    return dest_size + ALP_CODEC_HEADER_SIZE;
}

UInt32 CompressionCodecALP::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < ALP_CODEC_HEADER_SIZE)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP-encoded data, data has wrong header");

    const UInt8 meta_byte = static_cast<UInt8>(*source++);
    const UInt8 codec_version = meta_byte & 0x0F;
    const UInt8 codec_variant = meta_byte >> 4 & 0x01;

    if (codec_version != ALP_CODEC_VERSION)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ALP-encoded data, unsupported codec version {}",
            static_cast<UInt32>(codec_version));

    if (codec_variant != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ALP-encoded data, unsupported codec variant {}",
            static_cast<UInt32>(codec_variant));

    const UInt8 data_float_width = static_cast<UInt8>(*source++);

    const UInt16 block_float_count = unalignedLoadLittleEndian<UInt16>(source);
    source += sizeof(UInt16);
    if (block_float_count != ALP_BLOCK_MAX_FLOAT_COUNT)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ALP-encoded data, supported block float count is {}, got {}",
            ALP_BLOCK_MAX_FLOAT_COUNT, block_float_count);

    source_size -= ALP_CODEC_HEADER_SIZE;
    UInt32 dest_size;

    switch (data_float_width)
    {
    case sizeof(Float32):
        dest_size = ALPCodecDecoder<Float32>().decode(source, source_size, dest, uncompressed_size);
        break;
    case sizeof(Float64):
        dest_size = ALPCodecDecoder<Float64>().decode(source, source_size, dest, uncompressed_size);
        break;
    default:
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ALP-encoded data, unsupported float width {}",
            static_cast<size_t>(data_float_width));
    }

    return dest_size;
}

void registerCodecALP(CompressionCodecFactory & factory)
{
    auto method_code = static_cast<UInt8>(CompressionMethodByte::ALP);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        if (arguments && !arguments->children.empty())
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                "ALP codec must have 0 parameters, given {}",
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
        return std::make_shared<CompressionCodecALP>(float_width);
    };
    factory.registerCompressionCodecWithType("ALP", method_code, codec_builder);
}
}
