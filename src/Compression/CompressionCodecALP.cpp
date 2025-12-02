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

/** ALP column codec implementation.
 *
 * Based on ALP paper: https://ir.cwi.nl/pub/33334
 *
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

    String getDescription() const override
    {
        return "Adaptive Lossless floating-Point; suitable for time series data.";
    }

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
 * ALP header = 2 bytes
 * ┌───────────────────────────────────┬───────────────────────────────────┐
 * │             Byte 0                │             Byte 1                │
 * ├─────────────────┬─────────────────┼───────────────────────────────────┤
 * │ 4 bits: Version │ 1 bit: Variant  │ 8 bits: Float Width (in bytes)    │
 * │    (0-15)       │ (0=ALP,1=ALPRD) │    (4 or 8)                       │
 * └─────────────────┴─────────────────┴───────────────────────────────────┘
 */
constexpr size_t ALP_CODEC_HEADER_SIZE = 2 * sizeof(UInt8);

/**
 * ALP block header = 15 bytes
 * ┌────────────────────────────────────────────────┐
 * │ Bytes 0-1 (2 bytes)                            │
 * ├───────────┬────────────────────────────────────┤
 * │ 1 bit     │                                    │
 * │Compression│ 15 bits: Float Count (0-32767)     │
 * │ Flag      │                                    │
 * └───────────┴────────────────────────────────────┘
 * ┌────────────────────────────────────────────────┐
 * │ Byte 2: Magnitude Index (1 byte)               │
 * └────────────────────────────────────────────────┘
 * ┌────────────────────────────────────────────────┐
 * │ Byte 3: Fraction Index (1 byte)                │
 * └────────────────────────────────────────────────┘
 * ┌────────────────────────────────────────────────┐
 * │ Bytes 4-5: Exception Count (2 bytes)           │
 * └────────────────────────────────────────────────┘
 * ┌────────────────────────────────────────────────┐
 * │ Byte 6: Frame of Reference Bit-Width (1 byte)  │
 * └────────────────────────────────────────────────┘
 * ┌────────────────────────────────────────────────┐
 * │ Bytes 7-14: Frame of Reference Value (8 bytes) │
 * └────────────────────────────────────────────────┘
 * Compression Flag indicates whether the block is compressed (0) or uncompressed (1).
 * If uncompressed, the block contains raw float values following header byte 1 (no remaining header bytes).
 */
constexpr size_t ALP_BLOCK_HEADER_SIZE = 15 * sizeof(UInt8);

constexpr size_t ALP_MAX_FLOATS_IN_BLOCK = 1024; // 2 << 15;

constexpr double ALP_COMPRESSION_THRESHOLD = 0.9;

constexpr UInt32 PARAMS_ESTIMATIONS_SAMPLES = 8;
constexpr UInt32 PARAMS_ESTIMATIONS_SAMPLE_FLOATS = 32;
constexpr UInt32 PARAMS_ESTIMATIONS_CANDIDATES = 5;

template <typename T, UInt32 N, bool inverse>
requires std::is_floating_point_v<T>
constexpr std::array<T, N> generatePowersOf10()
{
    std::array<T, N> arr{};
    for (UInt32 i = 0, v = 1; i < N; ++i, v *= 10)
        arr[i] = inverse ? static_cast<T>(1) / static_cast<T>(v) : static_cast<T>(v);
    return arr;
}

template<typename T>
requires std::is_floating_point_v<T>
class ALPCodecNumbers;

template<>
class ALPCodecNumbers<Float64>
{
public:
    static constexpr std::array<Float64, 19> MAGNITUDES = generatePowersOf10<Float64, 19, false>();
    static constexpr std::array<Float64, 19> FRACTIONS = generatePowersOf10<Float64, 19, true>();

    static constexpr Float64 UPPER_LIMIT = 9223372036854774784.0;
    static constexpr Float64 LOWER_LIMIT = -9223372036854774784.0;

    static constexpr double CORRECTION_NUMBER = 6755399441055744.0; // 2^51 + 2^52
};

template<>
class ALPCodecNumbers<Float32>
{
public:
    static constexpr std::array<Float32, 11> MAGNITUDES = generatePowersOf10<Float32, 11, false>();
    static constexpr std::array<Float32, 11> FRACTIONS = generatePowersOf10<Float32, 11, true>();

    static constexpr Float32 UPPER_LIMIT = 9223372036854774784.0f;
    static constexpr Float32 LOWER_LIMIT = -9223372036854774784.0f;

    static constexpr float CORRECTION_NUMBER = 12582912.0; // 2^22 + 2^23
};

template <typename T>
requires std::is_floating_point_v<T>
class ALPCodec
{
protected:
    static Int64 encodeValue(T value, UInt8 magnitude_index, UInt8 fraction_index)
    {
        T multiplier = ALPCodecNumbers<T>::MAGNITUDES[magnitude_index] * ALPCodecNumbers<T>::FRACTIONS[fraction_index];
        T value_enc = value * multiplier;

        const bool invalid = std::isinf(value_enc) || std::isnan(value_enc) ||
            value_enc < ALPCodecNumbers<T>::LOWER_LIMIT || value_enc > ALPCodecNumbers<T>::UPPER_LIMIT ||
                (value_enc == static_cast<T>(0.0) && std::signbit(value_enc));
        if (unlikely(invalid))
            return static_cast<Int64>(ALPCodecNumbers<T>::UPPER_LIMIT);

        value_enc = value_enc + ALPCodecNumbers<T>::CORRECTION_NUMBER - ALPCodecNumbers<T>::CORRECTION_NUMBER;

        return static_cast<Int64>(value_enc);
    }

    static T decodeValue(Int64 value, UInt8 magnitude_index, UInt8 fraction_index)
    {
        auto magnitude = ALPCodecNumbers<T>::MAGNITUDES[fraction_index];
        auto fraction = ALPCodecNumbers<T>::FRACTIONS[magnitude_index];
        T multiplier = magnitude * fraction;
        T value_dec = static_cast<T>(value) * multiplier;
        return value_dec;
    }
};

template <typename T>
requires std::is_floating_point_v<T>
class ALPCodecEncoder final : ALPCodec<T>
{
public:
    explicit ALPCodecEncoder()
        : block_encoded(ALP_MAX_FLOATS_IN_BLOCK), block_exceptions(),
          block_encoded_min(0), block_encoded_max(0), block_for_bit_width(0)
    {
    }

    char * encode(const char * source, UInt32 source_size, char * dest)
    {
        assert(source_size % sizeof(T) == 0);
        UInt32 float_count = source_size / sizeof(T);

        estimateParamCandidates(source, float_count);

        while (float_count > 0)
        {
            const UInt32 floats_in_block = std::min<UInt32>(float_count, ALP_MAX_FLOATS_IN_BLOCK);

            dest = encodeBlock(source, floats_in_block, dest);

            source += floats_in_block * sizeof(T);
            float_count -= floats_in_block;
        }

        return dest;
    }
private:
    struct EncodingParams
    {
        UInt8 magnitude_index;
        UInt8 fraction_index;
        size_t occurred_times;
    };

    struct EncodingException
    {
        UInt16 index;
        T value;
    };

    std::vector<EncodingParams> param_candidates;
    std::vector<Int64> block_encoded;
    std::vector<EncodingException> block_exceptions;
    Int64 block_encoded_min;
    Int64 block_encoded_max;
    size_t block_for_bit_width;

    char * encodeBlock(const char * source, const UInt16 float_count, char * dest)
    {
        const char * source_start = source;
        const size_t uncompressed_block_size = float_count * sizeof(T);

        const EncodingParams params = estimateParams(source, float_count);

        block_encoded.clear();
        block_exceptions.clear();

        block_encoded_min = std::numeric_limits<Int64>::max();
        block_encoded_max = std::numeric_limits<Int64>::min();

        for (UInt16 i = 0; i < float_count; ++i, source += sizeof(T))
        {
            const T value = unalignedLoadLittleEndian<T>(source);
            const Int64 value_enc = ALPCodec<T>::encodeValue(value, params.magnitude_index, params.fraction_index);
            const T value_dec = ALPCodec<T>::decodeValue(value_enc, params.magnitude_index, params.fraction_index);

            if (likely(std::abs(value - value_dec) < std::numeric_limits<T>::epsilon()))
            {
                block_encoded.push_back(value_enc);
                block_encoded_min = std::min(value_enc, block_encoded_min);
                block_encoded_max = std::max(value_enc, block_encoded_max);
            }
            else
                block_exceptions.push_back({i, value});
        }

        block_for_bit_width = getFrameOfReferenceBitWidth(block_encoded_min, block_encoded_max);

        const size_t encoded_bits = block_encoded.size() * block_for_bit_width;
        const size_t encoded_bytes = (encoded_bits + 7) / 8;

        const size_t estimated_compression_size =
            ALP_BLOCK_HEADER_SIZE +
            encoded_bytes +
            static_cast<UInt32>(block_exceptions.size()) * (sizeof(UInt16) + sizeof(T));
        if (estimated_compression_size > static_cast<size_t>(uncompressed_block_size * ALP_COMPRESSION_THRESHOLD))
        {
            // Write uncompressed block
            // Compression Flag = 1 (uncompressed)
            unalignedStoreLittleEndian<UInt16>(dest, static_cast<UInt16>(0x8000 | float_count));
            dest += sizeof(UInt16);

            memcpy(dest, source_start, uncompressed_block_size);
            dest += uncompressed_block_size;

            return dest;
        }

        // Compression Float Count
        unalignedStoreLittleEndian<UInt16>(dest, float_count);
        dest += sizeof(UInt16);

        // Magnitude and Fraction Indices
        *dest++ = params.magnitude_index;
        *dest++ = params.fraction_index;

        // Exception Count
        unalignedStoreLittleEndian<UInt16>(dest, static_cast<UInt16>(block_exceptions.size()));
        dest += sizeof(UInt16);

        // Frame of Reference Bit-Width
        *dest++ = static_cast<UInt8>(block_for_bit_width);

        // Frame of Reference Value
        unalignedStoreLittleEndian<Int64>(dest, block_encoded_min);
        dest += sizeof(Int64);

        BitWriter bit_writer(dest, encoded_bytes);

        for (const auto & enc_value : block_encoded)
        {
            const Int64 for_enc_value = static_cast<UInt64>(enc_value - block_encoded_min);
            bit_writer.writeBits(block_for_bit_width, for_enc_value);
        }
        bit_writer.flush();
        assert(bit_writer.count() / 8 == encoded_bytes);
        dest += encoded_bytes;

        for (const auto & exception : block_exceptions)
        {
            unalignedStoreLittleEndian<UInt16>(dest, exception.index);
            dest += sizeof(UInt16);

            unalignedStoreLittleEndian<T>(dest, exception.value);
            dest += sizeof(T);
        }

        return dest;
    }

    EncodingParams estimateParams(const char * source, const UInt32 float_count)
    {
        assert(param_candidates.size() > 0);

        char * sample[PARAMS_ESTIMATIONS_SAMPLE_FLOATS * sizeof(T)];
        const UInt32 sample_count = std::min<UInt32>(float_count, PARAMS_ESTIMATIONS_SAMPLE_FLOATS);
        const UInt32 sample_step = std::max(float_count / sample_count, 1u);
        for (UInt32 i = 0; i < sample_count; ++i)
            unalignedStoreLittleEndian<T>(&sample[i * sizeof(T)], unalignedLoadLittleEndian<T>(&source[i * sample_step * sizeof(T)]));

        EncodingParams best_params = {0, 0, 0};
        size_t best_size = std::numeric_limits<size_t>::max();
        uint worse_counter = 0;

        for (const auto & params : param_candidates)
        {
            const size_t estimated_size = estimateEncodingSize(reinterpret_cast<char *>(sample), sample_count, params);

            if (estimated_size < best_size)
            {
                best_size = estimated_size;
                best_params = params;
                worse_counter = 0;
            }
            else if (++worse_counter >= 2)
                break;
        }

        return best_params;
    }

    void estimateParamCandidates(const char * source, const UInt32 float_count)
    {
        std::unordered_map<UInt16, EncodingParams> params_map;

        UInt32 sample_step = float_count > PARAMS_ESTIMATIONS_SAMPLES * PARAMS_ESTIMATIONS_SAMPLE_FLOATS
            ? (float_count - PARAMS_ESTIMATIONS_SAMPLES * PARAMS_ESTIMATIONS_SAMPLE_FLOATS) / PARAMS_ESTIMATIONS_SAMPLES
            : PARAMS_ESTIMATIONS_SAMPLE_FLOATS;

        for (UInt32 i = 0; i < PARAMS_ESTIMATIONS_SAMPLES; ++i)
        {
            const UInt32 sample_start_float = i * sample_step;
            if (sample_start_float >= float_count)
                break;

            EncodingParams best_params = {0, 0, 0};
            size_t best_size = std::numeric_limits<size_t>::max();

            const char * sample_start = source + sample_start_float * sizeof(T);
            const UInt16 sample_float_count = std::min<UInt16>(PARAMS_ESTIMATIONS_SAMPLE_FLOATS, float_count - sample_start_float);

            for (UInt8 magnitude_index = 0; magnitude_index < static_cast<UInt8>(ALPCodecNumbers<T>::MAGNITUDES.size()); ++magnitude_index)
            {
                for (UInt8 fraction_index = 0; fraction_index <= magnitude_index; ++fraction_index)
                {
                    const EncodingParams params{magnitude_index, fraction_index, 1};
                    const size_t estimated_size = estimateEncodingSize(sample_start, sample_float_count, params);

                    if (estimated_size < best_size)
                    {
                        best_size = estimated_size;
                        best_params = params;
                    }
                }
            }

            const UInt16 key = (static_cast<UInt16>(best_params.magnitude_index) << 8) | static_cast<UInt16>(best_params.fraction_index);
            auto it = params_map.find(key);
            if (it != params_map.end())
                ++(it->second.occurred_times);
            else
                params_map[key] = best_params;
        }

        param_candidates.clear();
        for (const auto & [_, params] : params_map)
            param_candidates.push_back(params);

        std::sort(param_candidates.begin(), param_candidates.end(),
            [](const EncodingParams & a, const EncodingParams & b)
            {
                if (a.occurred_times == b.occurred_times)
                {
                    if (a.magnitude_index == b.magnitude_index)
                        return a.fraction_index < b.fraction_index;
                    return a.magnitude_index < b.magnitude_index;
                }
                return a.occurred_times < b.occurred_times;
            });

        param_candidates.resize(std::min<size_t>(param_candidates.size(), PARAMS_ESTIMATIONS_CANDIDATES));
    }

    size_t estimateEncodingSize(const char * source, const UInt16 float_count, const EncodingParams & params)
    {
        Int64 enc_min = std::numeric_limits<Int64>::max();
        Int64 enc_max = std::numeric_limits<Int64>::min();
        size_t exception_count = 0;

        for (const char * source_end = source + float_count * sizeof(T); source < source_end; source += sizeof(T))
        {
            const T value = unalignedLoadLittleEndian<T>(source);
            const Int64 value_enc = ALPCodec<T>::encodeValue(value, params.magnitude_index, params.fraction_index);
            const T value_dec = ALPCodec<T>::decodeValue(value_enc, params.magnitude_index, params.fraction_index);

            if (likely(std::abs(value - value_dec) < std::numeric_limits<T>::epsilon()))
            {
                enc_min = std::min(value_enc, enc_min);
                enc_max = std::max(value_enc, enc_max);
            }
            else
                ++exception_count;
        }

        const size_t values_enc_bits = (float_count - exception_count) * getFrameOfReferenceBitWidth(enc_min, enc_max);
        const size_t values_enc_bytes = (values_enc_bits + 7) / 8;

        const size_t total_size = ALP_BLOCK_HEADER_SIZE + values_enc_bytes + exception_count * (sizeof(UInt16) + sizeof(T));

        return total_size;
    }

    static size_t getFrameOfReferenceBitWidth(const Int64 min_value, const Int64 max_value)
    {
        const Int64 values_diff = max_value - min_value;
        if (values_diff == 0)
            return 1; // Edge case when all values are identical, need at least 1 bit to represent.

        const size_t bit_width = sizeof(Int64) * 8 - getLeadingZeroBitsUnsafe<UInt64>(static_cast<UInt64>(values_diff));
        return bit_width;
    }
};

template <typename T>
requires std::is_floating_point_v<T>
class ALPCodecDecoder final : ALPCodec<T>
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

        const UInt16 compression_flag_and_count = unalignedLoadLittleEndian<UInt16>(source);
        source += sizeof(UInt16);

        const bool is_uncompressed = (compression_flag_and_count & 0x8000) != 0;
        const UInt16 float_count = compression_flag_and_count & 0x7FFF;

        if (is_uncompressed)
        {
            const size_t uncompressed_block_size = float_count * sizeof(T);
            if (source + uncompressed_block_size > source_end || dest + uncompressed_block_size > dest_end)
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP-encoded data. Incomplete uncompressed block");

            memcpy(dest, source, uncompressed_block_size);
            source += uncompressed_block_size;
            dest += uncompressed_block_size;

            return;
        }

        const UInt8 magnitude_index = static_cast<UInt8>(*source++);
        const UInt8 fraction_index = static_cast<UInt8>(*source++);

        const UInt16 exception_count = unalignedLoadLittleEndian<UInt16>(source);
        const UInt16 valid_float_count = float_count - exception_count;
        source += sizeof(UInt16);

        const UInt8 for_bit_width = static_cast<UInt8>(*source++);
        const Int64 for_value = unalignedLoadLittleEndian<Int64>(source);
        source += sizeof(Int64);

        size_t encoded_bits = static_cast<size_t>(valid_float_count) * static_cast<size_t>(for_bit_width);
        size_t encoded_bytes = encoded_bits % 8 == 0 ? encoded_bits / 8 : encoded_bits / 8 + 1;
        BitReader bit_reader(source, encoded_bytes);

        source += encoded_bytes;

        UInt16 remaining_valid_floats = valid_float_count;
        UInt16 remaining_exceptions = exception_count;

        UInt16 value_index = 0;
        UInt16 exception_index;
        T exception_value;
        if (remaining_exceptions > 0)
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

        for (; remaining_valid_floats > 0; ++value_index)
        {
            if (value_index == exception_index)
            {
                unalignedStoreLittleEndian<T>(dest, exception_value);
                dest += sizeof(T);
                --remaining_exceptions;
                if (remaining_exceptions > 0)
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
                const UInt64 for_enc_value = bit_reader.readBits(for_bit_width);
                const Int64 encoded_value = for_value + static_cast<Int64>(for_enc_value);

                const T decoded_value = ALPCodec<T>::decodeValue(encoded_value, magnitude_index, fraction_index);
                unalignedStoreLittleEndian<T>(dest, decoded_value);
                dest += sizeof(T);
                --remaining_valid_floats;
            }
        }

        if (remaining_exceptions > 0)
        {
            unalignedStoreLittleEndian<T>(dest, exception_value);
            dest += sizeof(T);
            --remaining_exceptions;

            while (remaining_exceptions-- > 0)
            {
                source += sizeof(UInt16);

                exception_value = unalignedLoadLittleEndian<T>(source);
                source += sizeof(T);

                unalignedStoreLittleEndian<T>(dest, exception_value);
                dest += sizeof(T);
            }
        }
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
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecALP::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return uncompressed_size +
        ALP_CODEC_HEADER_SIZE +
        (uncompressed_size / float_width / ALP_MAX_FLOATS_IN_BLOCK + 1) * ALP_BLOCK_HEADER_SIZE;
}

UInt32 CompressionCodecALP::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    char * dest_start = dest;

    // Write ALP header
    constexpr UInt8 meta_byte = (static_cast<UInt8>(0) << 4) | (ALP_CODEC_VERSION & 0x0F);
    *dest++ = meta_byte;
    *dest++ = float_width;

    switch (float_width)
    {
    case sizeof(Float32):
        dest = ALPCodecEncoder<Float32>().encode(source, source_size, dest);
        break;
    case sizeof(Float64):
        dest = ALPCodecEncoder<Float64>().encode(source, source_size, dest);
        break;
    default:
        throw Exception(ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with codec ALP. Unsupported float width {}",
            static_cast<size_t>(float_width));
    }

    return static_cast<UInt32>(dest - dest_start);
}

void CompressionCodecALP::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < ALP_CODEC_HEADER_SIZE)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ALP-encoded data. File has wrong header");

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
