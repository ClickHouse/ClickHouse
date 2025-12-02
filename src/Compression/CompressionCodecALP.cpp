#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>

#include <Parsers/IAST.h>
#include <DataTypes/IDataType.h>

#include <base/unaligned.h>

#include <IO/WriteHelpers.h>
#include <IO/BitHelpers.h>

#include <functional>
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
 *   [ALP codec header]  2 bytes, written once per compressed column
 *   [ALP block 0]
 *   [ALP block 1]
 *   ...
 *   [ALP block N-1]
 *
 * ALP codec header (2 bytes):
 *   - meta_byte:
 *     - low 4 bits: ALP codec version (currently 1)
 *     - bit 4:      ALP variant (0 = ALP, 1 = ALP_RD; only 0 is supported)
 *   - float_width:
 *     - 4 or 8 bytes (Float32 / Float64)
 *
 * The input column is split into blocks of up to ALP_MAX_FLOATS_IN_BLOCK values (1024).
 * Each block is encoded independently and can be either compressed or left raw, depending on the estimated gain.
 *
 * Core Idea: Decimal-Based Integerization
 * The ALP paper observes that most stored doubles originate as decimals. For a block, we try to represent each float value as an integer:
 *   1) multiplier(e, f) = 10^e * 10^(-f)
 *   2) d = (Int64) round(v * multiplier(e, f))
 * where:
 *   - e controls the decimal scaling up
 *   - f controls how many trailing decimal zeros we effectively “cut off” again
 * A value is considered exactly encodable if:
 *   decodeValue(encodeValue(v, e, f), e, f) == v (within epsilon)
 * Encodable values become part of the integer stream; non-encodable values become exceptions stored verbatim.
 *
 * Two-Level Sampling to Select (e,f)
 * ALP’s adaptivity over a block is driven by a two-level sampling scheme:
 *   1) Global pre-sampling over the entire column:
 *     - Take PARAMS_ESTIMATIONS_SAMPLES disjoint sub-samples from the column (each up to PARAMS_ESTIMATIONS_SAMPLE_FLOATS values).
 *     - For each sub-sample, brute-force over all valid (e,f) pairs with 0 <= e < |MAGNITUDES| and 0 <= f <= e.
 *     - For each (e,f), estimate the encoded size and keep track of the best (e,f) for that sub-sample.
 *     - The result is a small, global candidate set of (e,f) pairs (up to 5) that are likely good for this column.
 *   2) Per-block refinement
 *     - For each block of up to 1024 values, take a local sample
 *     - Evaluate only the global candidate (e,f) list for that sample to choose the best (e,f) for that block.
 *
 * Per-Block Encoding Schema (Compressed Case)
 *   - 2 bytes: compression flag and numbers count (UInt16)
 *     * high bit 0 → compressed
 *     * low 15 bits → numbers count in block
 *   - 1 byte: exponent (e) (UInt8)
 *   - 1 byte: fraction (f) (UInt8)
 *   - 2 bytes: exceptions count (UInt16)
 *   - 1 byte:  FOR bit-width (UInt8)
 *   - 8 bytes: FOR base value (Int64)
 *   - Compressed block payload:
 *     * Bit-pack for each encoded value using bit-width bits.
 *     * For each exception:
 *       - UInt16 index (position in source block - offset)
 *       - raw value (float or double)
 *
 * Per-Block Encoding Schema (Uncompressed Case)
 *   - 2 bytes: compression flag and numbers count
 *     * high bit 1 → uncompressed
 *     * low 15 bits → numbers count in block
 *   - Raw numbers for the block
 *
 * Notes:
 *   - This codec implements the ALP variant only.
 *     ALP_RD (front-bits-based encoder for “real doubles”) is not implemented.
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
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
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

constexpr UInt32 ALP_CODEC_HEADER_SIZE = sizeof(UInt16);
constexpr UInt32 ALP_BLOCK_HEADER_SIZE = 15 * sizeof(UInt8);
constexpr UInt32 ALP_UNENCODED_BLOCK_HEADER_SIZE = sizeof(UInt16);

constexpr UInt32 ALP_MAX_BLOCK_FLOAT_COUNT = 1024;

constexpr double ALP_COMPRESSION_THRESHOLD = 0.9;

constexpr UInt32 ALP_PARAMS_ESTIMATION_SAMPLES = 8;
constexpr UInt32 ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS = 32;
constexpr UInt32 ALP_PARAMS_ESTIMATION_CANDIDATES = 5;

template <typename T>
concept FLOAT = std::is_same_v<T, Float32> || std::is_same_v<T, Float64>;

template <FLOAT T>
struct ALPFloatTraits;

template <FLOAT T, UInt32 N, bool inverse>
constexpr std::array<T, N> generatePowersOf10()
{
    std::array<T, N> arr{};
    for (UInt64 i = 0, v = 1; i < N; ++i, v *= 10)
        arr[i] = inverse ? static_cast<T>(1) / static_cast<T>(v) : static_cast<T>(v);
    return arr;
}

template<>
struct ALPFloatTraits<Float64>
{
    static constexpr UInt8 EXPONENT_COUNT = 18;

    static constexpr std::array<Float64, EXPONENT_COUNT> EXPONENTS = generatePowersOf10<Float64, EXPONENT_COUNT, false>();
    static constexpr std::array<Float64, EXPONENT_COUNT> FRACTIONS = generatePowersOf10<Float64, EXPONENT_COUNT, true>();

    static constexpr Float64 UPPER = 9223372036854774784.0;
    static constexpr Float64 LOWER = -9223372036854774784.0;

    static constexpr double ROUND_MAGIC = 6755399441055744.0; // 2^51 + 2^52
};

template<>
struct ALPFloatTraits<Float32>
{
    static constexpr UInt8 EXPONENT_COUNT = 11;

    static constexpr std::array<Float32, EXPONENT_COUNT> EXPONENTS = generatePowersOf10<Float32, EXPONENT_COUNT, false>();
    static constexpr std::array<Float32, EXPONENT_COUNT> FRACTIONS = generatePowersOf10<Float32, EXPONENT_COUNT, true>();

    static constexpr Float32 UPPER = 9223372036854774784.0f;
    static constexpr Float32 LOWER = -9223372036854774784.0f;

    static constexpr float ROUND_MAGIC = 12582912.0; // 2^22 + 2^23
};

template<FLOAT T>
struct ALPFloatUtils
{
    static Int64 encodeValue(T value, UInt8 exponent, UInt8 fraction)
    {
        T multiplier = ALPFloatTraits<T>::EXPONENTS[exponent] * ALPFloatTraits<T>::FRACTIONS[fraction];
        T value_enc = value * multiplier;

        const bool invalid = std::isinf(value_enc) ||
            std::isnan(value_enc) ||
                value_enc < ALPFloatTraits<T>::LOWER || value_enc > ALPFloatTraits<T>::UPPER ||
                    (value_enc == static_cast<T>(0.0) && std::signbit(value_enc));

        if (unlikely(invalid))
            return static_cast<Int64>(ALPFloatTraits<T>::UPPER);

        value_enc = value_enc + ALPFloatTraits<T>::ROUND_MAGIC - ALPFloatTraits<T>::ROUND_MAGIC;
        return static_cast<Int64>(value_enc);
    }

    static T decodeValue(Int64 value, UInt8 exponent, UInt8 fraction)
    {
        T multiplier = ALPFloatTraits<T>::EXPONENTS[fraction] * ALPFloatTraits<T>::FRACTIONS[exponent];
        T value_dec = static_cast<T>(value) * multiplier;
        return value_dec;
    }
};

template <FLOAT T>
class ALPCodecEncoder
{
public:
    explicit ALPCodecEncoder()
        : block_encoded(ALP_MAX_BLOCK_FLOAT_COUNT)
        , block_exceptions(ALP_MAX_BLOCK_FLOAT_COUNT)
        , block_frame_of_reference(0)
        , block_bit_width(0)
    {
    }

    UInt32 encode(const char * source, const UInt32 source_size, char * dest)
    {
        assert(source_size % sizeof(T) == 0);
        UInt32 float_count = source_size / sizeof(T);

        estimateParamCandidates(source, float_count);

        const char * dest_start = dest;
        while (float_count > 0)
        {
            const UInt32 block_floats_count = std::min<UInt32>(float_count, ALP_MAX_BLOCK_FLOAT_COUNT);

            dest = encodeBlock(source, block_floats_count, dest);

            source += block_floats_count * sizeof(T);
            float_count -= block_floats_count;
        }

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

    std::vector<EncodingParams> param_candidates;

    EncodingParams block_params;
    std::vector<Int64> block_encoded;
    std::vector<EncodingException> block_exceptions;
    Int64 block_frame_of_reference;
    size_t block_bit_width;

    char * encodeBlock(const char * source, const UInt16 float_count, char * dest)
    {
        encodeBlockToBuffer(source, float_count);

        const size_t encoded_bytes = (block_encoded.size() * block_bit_width + 7) / 8; // Estimated size of encoded values
        const size_t total_encoded_bytes = ALP_BLOCK_HEADER_SIZE + encoded_bytes + block_exceptions.size() * (sizeof(UInt16) + sizeof(T));
        const size_t total_unencoded_size = float_count * sizeof(T) + ALP_UNENCODED_BLOCK_HEADER_SIZE;
        if (total_encoded_bytes > static_cast<size_t>(total_unencoded_size * ALP_COMPRESSION_THRESHOLD)) // No significant compression gain
            return writeUnencoded(source, float_count, dest);

        // Write Compressed Block Header
        // Compression Float Count
        unalignedStoreLittleEndian<UInt16>(dest, float_count);
        dest += sizeof(UInt16);

        // Exponent and Fraction Indices
        *dest++ = block_params.exponent;
        *dest++ = block_params.fraction;

        // Exception Count
        unalignedStoreLittleEndian<UInt16>(dest, static_cast<UInt16>(block_exceptions.size()));
        dest += sizeof(UInt16);

        // Encoding Bit-Width
        *dest++ = static_cast<UInt8>(block_bit_width);

        // Frame of Reference Value
        unalignedStoreLittleEndian<Int64>(dest, block_frame_of_reference);
        dest += sizeof(Int64);

        // Write Encoded Values
        BitWriter bit_writer(dest, encoded_bytes);
        for (const auto & enc_value : block_encoded)
            bit_writer.writeBits(block_bit_width, static_cast<UInt64>(enc_value - block_frame_of_reference));
        bit_writer.flush();
        dest += encoded_bytes;
        assert(bit_writer.count() / 8 == encoded_bytes);

        // Write Exceptions
        for (const auto & exception : block_exceptions)
        {
            unalignedStoreLittleEndian<UInt16>(dest, exception.index);
            dest += sizeof(UInt16);

            unalignedStoreLittleEndian<T>(dest, exception.value);
            dest += sizeof(T);
        }

        return dest;
    }

    const char * encodeBlockToBuffer(const char * source, const UInt16 float_count)
    {
        block_params = selectBlockParams(source, float_count);

        block_encoded.clear();
        block_exceptions.clear();

        Int64 min = std::numeric_limits<Int64>::max();
        Int64 max = std::numeric_limits<Int64>::min();

        for (UInt16 i = 0; i < float_count; ++i, source += sizeof(T))
        {
            const T value = unalignedLoadLittleEndian<T>(source);
            const Int64 value_enc = ALPFloatUtils<T>::encodeValue(value, block_params.exponent, block_params.fraction);
            const T value_dec = ALPFloatUtils<T>::decodeValue(value_enc, block_params.exponent, block_params.fraction);

            if (likely(std::abs(value - value_dec) < std::numeric_limits<T>::epsilon()))
            {
                block_encoded.push_back(value_enc);
                min = std::min(value_enc, min);
                max = std::max(value_enc, max);
            }
            else
                block_exceptions.push_back({i, value});
        }

        block_frame_of_reference = min;
        block_bit_width = calculateBitWidth(min, max);

        return source;
    }

    static char * writeUnencoded(const char * source, const UInt16 float_count, char * dest)
    {
        const UInt16 block_header = static_cast<UInt16>(0x8000 | float_count); // high bit 1 → uncompressed
        unalignedStoreLittleEndian<UInt16>(dest, block_header);
        dest += sizeof(UInt16);

        const size_t block_size = float_count * sizeof(T);
        memcpy(dest, source, block_size);
        dest += block_size;

        return dest;
    }

    EncodingParams selectBlockParams(const char * source, const UInt32 float_count)
    {
        assert(param_candidates.size() > 0);

        // Sample up to ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS values from the block for local parameter estimation.
        // Evenly select sample values across the block and copy them into a temporary buffer for evaluation.
        char * sample[ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS * sizeof(T)];
        const UInt32 sample_count = std::min<UInt32>(float_count, ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS);
        const UInt32 sample_step = std::max(float_count / sample_count, 1u);
        for (UInt32 i = 0; i < sample_count; ++i)
            unalignedStoreLittleEndian<T>(&sample[i * sizeof(T)], unalignedLoadLittleEndian<T>(&source[i * sample_step * sizeof(T)]));

        EncodingParams best_params = {0, 0};
        size_t best_size = std::numeric_limits<size_t>::max();
        bool is_prev_worse = false;

        for (const auto & params : param_candidates)
        {
            const size_t estimated_size = estimateEncodingSize(reinterpret_cast<char *>(sample), sample_count, params);
            if (estimated_size < best_size)
            {
                best_size = estimated_size;
                best_params = params;
                is_prev_worse = false;
            }
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

        // For each sample, brute-force over all valid (exponent, fraction) pairs to find the best parameters for that sample.
        for (UInt32 i = 0; i < ALP_PARAMS_ESTIMATION_SAMPLES; ++i)
        {
            const UInt32 sample_start_index = i * sample_step;
            if (sample_start_index >= float_count)
                break;

            Estimation best_estimation = {{0, 0}, 0};
            size_t best_size = std::numeric_limits<size_t>::max();

            const char * sample_start = source + sample_start_index * sizeof(T);
            const UInt16 sample_float_count = std::min<UInt16>(ALP_PARAMS_ESTIMATION_SAMPLE_FLOATS, float_count - sample_start_index);

            for (UInt8 magnitude = 0; magnitude < ALPFloatTraits<T>::EXPONENT_COUNT; ++magnitude)
            {
                for (UInt8 fraction = 0; fraction <= magnitude; ++fraction)
                {
                    const Estimation estimation{{magnitude, fraction}, 1};
                    const size_t estimated_size = estimateEncodingSize(sample_start, sample_float_count, estimation.params);

                    if (estimated_size < best_size)
                    {
                        best_size = estimated_size;
                        best_estimation = estimation;
                    }
                }
            }

            const UInt16 key = (static_cast<UInt16>(best_estimation.params.exponent) << 8) | static_cast<UInt16>(best_estimation.params.fraction);
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

    size_t estimateEncodingSize(const char * source, const UInt32 float_count, const EncodingParams & params)
    {
        Int64 min = std::numeric_limits<Int64>::max();
        Int64 max = std::numeric_limits<Int64>::min();
        size_t exception_count = 0;

        for (const char * source_end = source + float_count * sizeof(T); source < source_end; source += sizeof(T))
        {
            const T value = unalignedLoadLittleEndian<T>(source);
            const Int64 value_enc = ALPFloatUtils<T>::encodeValue(value, params.exponent, params.fraction);
            const T value_dec = ALPFloatUtils<T>::decodeValue(value_enc, params.exponent, params.fraction);

            if (likely(std::abs(value - value_dec) < std::numeric_limits<T>::epsilon()))
            {
                min = std::min(value_enc, min);
                max = std::max(value_enc, max);
            }
            else
                ++exception_count;
        }

        const size_t values_enc_bits = (float_count - exception_count) * calculateBitWidth(min, max);
        const size_t values_enc_bytes = (values_enc_bits + 7) / 8;

        const size_t total_size = ALP_BLOCK_HEADER_SIZE + values_enc_bytes + exception_count * (sizeof(UInt16) + sizeof(T));
        return total_size;
    }

    static UInt8 calculateBitWidth(const Int64 min_value, const Int64 max_value)
    {
        const Int64 values_diff = max_value - min_value;
        if (values_diff == 0)
            return 1; // Edge case when all values are identical, need at least 1 bit to represent.

        const auto bit_width = sizeof(Int64) * 8 - getLeadingZeroBitsUnsafe<UInt64>(static_cast<UInt64>(values_diff));
        return static_cast<UInt8>(bit_width);
    }
};

template <FLOAT T>
class ALPCodecDecoder
{
public:
    void decode(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
    {
        assert(uncompressed_size % sizeof(T) == 0);

        const char * source_end = source + source_size;
        const char * dest_end = dest + uncompressed_size;

        while (source < source_end)
            decodeBlock(source, source_end, dest, dest_end);

        assert(source == source_end);
        assert(dest == dest_end);
    }

private:
    void decodeBlock(const char * & source, const char * source_end, char * & dest, const char * dest_end)
    {
        if (source + ALP_BLOCK_HEADER_SIZE > source_end)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP-encoded data. Incomplete block header");

        const UInt16 header_first_word = unalignedLoadLittleEndian<UInt16>(source);
        source += sizeof(UInt16);

        const bool is_unencoded = (header_first_word & 0x8000) != 0;
        const UInt16 float_count = header_first_word & 0x7FFF;

        if (is_unencoded)
        {
            processUnencodedBlock(source, source_end, dest, dest_end, float_count);
            return;
        }

        const UInt8 exponent = static_cast<UInt8>(*source++);
        const UInt8 fraction = static_cast<UInt8>(*source++);

        const UInt16 exception_count = unalignedLoadLittleEndian<UInt16>(source);
        const UInt16 encoded_float_count = float_count - exception_count;
        source += sizeof(UInt16);

        const UInt8 bit_width = static_cast<UInt8>(*source++);

        const Int64 frame_of_reference = unalignedLoadLittleEndian<Int64>(source);
        source += sizeof(Int64);

        // Prepare to read encoded values
        size_t encoded_bits = static_cast<size_t>(encoded_float_count) * static_cast<size_t>(bit_width);
        size_t encoded_bytes = (encoded_bits + 7) / 8;
        BitReader bit_reader(source, encoded_bytes);

        // Move source pointer past encoded values, to the exceptions section
        source += encoded_bytes;

        UInt16 remaining_encoded_float_count = encoded_float_count;
        UInt16 remaining_exception_count = exception_count;

        UInt16 value_index = 0;

        // Keep track of the next exception to insert
        UInt16 exception_index;
        T exception_value;
        if (remaining_exception_count > 0)
        {
            exception_index = unalignedLoadLittleEndian<UInt16>(source);
            source += sizeof(UInt16);
            exception_value = unalignedLoadLittleEndian<T>(source);
            source += sizeof(T);
        }
        else
        {
            exception_index = UINT16_MAX;
            exception_value = static_cast<T>(0);
        }

        for (; remaining_encoded_float_count > 0; ++value_index)
        {
            if (value_index == exception_index)
            {
                unalignedStoreLittleEndian<T>(dest, exception_value);
                dest += sizeof(T);

                --remaining_exception_count;

                if (remaining_exception_count > 0)
                {
                    exception_index = unalignedLoadLittleEndian<UInt16>(source);
                    source += sizeof(UInt16);

                    exception_value = unalignedLoadLittleEndian<T>(source);
                    source += sizeof(T);
                }
                else
                {
                    exception_index = UINT16_MAX;
                    exception_value = static_cast<T>(0);
                }
            }
            else
            {
                const Int64 encoded_value = frame_of_reference + static_cast<Int64>(bit_reader.readBits(bit_width));
                const T decoded_value = ALPFloatUtils<T>::decodeValue(encoded_value, exponent, fraction);

                unalignedStoreLittleEndian<T>(dest, decoded_value);
                dest += sizeof(T);

                --remaining_encoded_float_count;
            }
        }

        if (remaining_exception_count > 0)
        {
            unalignedStoreLittleEndian<T>(dest, exception_value);
            dest += sizeof(T);

            --remaining_exception_count;

            while (remaining_exception_count > 0)
            {
                source += sizeof(UInt16); // Skip exception index
                exception_value = unalignedLoadLittleEndian<T>(source);
                source += sizeof(T);

                unalignedStoreLittleEndian<T>(dest, exception_value);
                dest += sizeof(T);

                --remaining_exception_count;
            }
        }
    }

    static void processUnencodedBlock(const char * & source, const char * source_end, char * & dest, const char * dest_end, const UInt16 float_count)
    {
        const size_t block_size = float_count * sizeof(T);
        if (source + block_size > source_end || dest + block_size > dest_end)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP-encoded data. Incomplete uncompressed block");

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
    const UInt32 num_blocks = uncompressed_size / float_width / ALP_MAX_BLOCK_FLOAT_COUNT + 1;
    return uncompressed_size + ALP_CODEC_HEADER_SIZE + num_blocks * ALP_BLOCK_HEADER_SIZE;
}

UInt32 CompressionCodecALP::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    // Write ALP header
    *dest++ = ALP_CODEC_VERSION; // meta_byte: version = 1, variant = 0
    *dest++ = float_width;

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
            "Cannot compress with codec ALP. Unsupported float width {}",
            static_cast<size_t>(float_width));
    }

    return dest_size + ALP_CODEC_HEADER_SIZE;
}

void CompressionCodecALP::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < ALP_CODEC_HEADER_SIZE)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP-encoded data. File has wrong header");

    // Read ALP header
    const UInt8 meta_byte = static_cast<UInt8>(*source++);
    const UInt8 codec_version = meta_byte & 0x0F;
    const UInt8 codec_variant = meta_byte >> 4 & 0x01;

    if (codec_version != ALP_CODEC_VERSION)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ALP-encoded data. Unsupported codec version {}",
            static_cast<UInt32>(codec_version));

    if (codec_variant != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ALP-encoded data. Unsupported codec variant {}",
            static_cast<UInt32>(codec_variant));

    switch (const UInt8 data_float_width = static_cast<UInt8>(*source++))
    {
    case sizeof(Float32):
        ALPCodecDecoder<Float32>().decode(source, source_size - ALP_CODEC_HEADER_SIZE, dest, uncompressed_size);
        break;
    case sizeof(Float64):
        ALPCodecDecoder<Float64>().decode(source, source_size - ALP_CODEC_HEADER_SIZE, dest, uncompressed_size);
        break;
    default:
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ALP-encoded data. Unsupported float width {}",
            static_cast<size_t>(data_float_width));
    }
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

            float_width = column_type->getSizeOfValueInMemory();
        }
        return std::make_shared<CompressionCodecALP>(float_width);
    };
    factory.registerCompressionCodecWithType("ALP", method_code, codec_builder);
}
}
