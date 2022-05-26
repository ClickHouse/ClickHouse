#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>

#include <span>
#include <bit>
#include <concepts>


namespace DB
{

class CompressionCodecFPC : public ICompressionCodec
{
public:
    explicit CompressionCodecFPC(UInt8 float_size, UInt8 compression_level);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }

    static constexpr UInt32 HEADER_SIZE{3};

private:
    UInt8 float_width;
    UInt8 level;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int BAD_ARGUMENTS;
}

uint8_t CompressionCodecFPC::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::FPC);
}

void CompressionCodecFPC::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

CompressionCodecFPC::CompressionCodecFPC(UInt8 float_size, UInt8 compression_level)
    : float_width{float_size}, level{compression_level}
{
    setCodecDescription("FPC", {std::make_shared<ASTLiteral>(static_cast<UInt64>(level))});
}

UInt32 CompressionCodecFPC::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    auto float_count = (uncompressed_size + float_width - 1) / float_width;
    if (float_count % 2 != 0) {
        ++float_count;
    }
    return HEADER_SIZE + float_count * float_width + float_count / 2;
}

namespace
{

UInt8 getFloatBytesSize(const IDataType & column_type)
{
    if (!WhichDataType(column_type).isFloat())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "FPC codec is not applicable for {} because the data type is not float",
                        column_type.getName());
    }

    if (auto float_size = column_type.getSizeOfValueInMemory(); float_size >= 4)
    {
        return static_cast<UInt8>(float_size);
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "FPC codec is not applicable for floats of size less than 4 bytes. Given type {}",
                    column_type.getName());
}

UInt8 encodeEndianness(std::endian endian)
{
    switch (endian)
    {
        case std::endian::little:
            return 0;
        case std::endian::big:
            return 1;
    }
    throw Exception("Unsupported endianness", ErrorCodes::BAD_ARGUMENTS);
}

std::endian decodeEndianness(UInt8 endian) {
    switch (endian)
    {
        case 0:
            return std::endian::little;
        case 1:
            return std::endian::big;
    }
    throw Exception("Unsupported endianness", ErrorCodes::BAD_ARGUMENTS);
}

}

void registerCodecFPC(CompressionCodecFactory & factory)
{
    auto method_code = static_cast<UInt8>(CompressionMethodByte::FPC);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        UInt8 float_width{0};
        if (column_type != nullptr)
            float_width = getFloatBytesSize(*column_type);

        UInt8 level{12};
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
            {
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                                "FPC codec must have 1 parameter, given {}", arguments->children.size());
            }

            const auto * literal = arguments->children.front()->as<ASTLiteral>();
            if (!literal)
                throw Exception("FPC codec argument must be integer", ErrorCodes::ILLEGAL_CODEC_PARAMETER);

            level = literal->value.safeGet<UInt8>();
        }
        return std::make_shared<CompressionCodecFPC>(float_width, level);
    };
    factory.registerCompressionCodecWithType("FPC", method_code, codec_builder);
}

namespace
{

template <std::unsigned_integral TUint> requires (sizeof(TUint) >= 4)
class DfcmPredictor {
public:
    explicit DfcmPredictor(std::size_t table_size): table(table_size, 0), prev_value{0}, hash{0}
    {
    }

    [[nodiscard]]
    TUint predict() const noexcept
    {
        return table[hash] + prev_value;
    }

    void add(TUint value) noexcept
    {
        table[hash] = value - prev_value;
        recalculateHash();
        prev_value = value;
    }

private:
    void recalculateHash() noexcept
    {
        auto value = table[hash];
        if constexpr (sizeof(TUint) >= 8)
        {
            hash = ((hash << 2) ^ static_cast<std::size_t>(value >> 40)) & (table.size() - 1);
        }
        else
        {
            hash = ((hash << 4) ^ static_cast<std::size_t>(value >> 23)) & (table.size() - 1);
        }
    }

    std::vector<TUint> table;
    TUint prev_value{0};
    std::size_t hash{0};
};

template <std::unsigned_integral TUint> requires (sizeof(TUint) >= 4)
class FcmPredictor {
public:
    explicit FcmPredictor(std::size_t table_size): table(table_size, 0), hash{0}
    {
    }

    [[nodiscard]]
    TUint predict() const noexcept
    {
        return table[hash];
    }

    void add(TUint value) noexcept
    {
        table[hash] = value;
        recalculateHash();
    }

private:
    void recalculateHash() noexcept
    {
        auto value = table[hash];
        if constexpr (sizeof(TUint) >= 8)
        {
            hash = ((hash << 6) ^ static_cast<std::size_t>(value >> 48)) & (table.size() - 1);
        }
        else
        {
            hash = ((hash << 1) ^ static_cast<std::size_t>(value >> 22)) & (table.size() - 1);
        }
    }

    std::vector<TUint> table;
    std::size_t hash{0};
};

template <std::unsigned_integral TUint, std::endian Endian = std::endian::native>
    requires (Endian == std::endian::little || Endian == std::endian::big)
class FPCOperation
{
    static constexpr std::size_t CHUNK_SIZE{64};

    static constexpr auto VALUE_SIZE = sizeof(TUint);
    static constexpr std::byte DFCM_BIT_1{1u << 7};
    static constexpr std::byte DFCM_BIT_2{1u << 3};
    static constexpr unsigned MAX_COMPRESSED_SIZE{0b111u};

public:
    explicit FPCOperation(std::span<std::byte> destination, UInt8 compression_level)
        : dfcm_predictor(1 << compression_level), fcm_predictor(1 << compression_level), chunk{}, result{destination}
    {
    }

    std::size_t encode(std::span<const std::byte> data) &&
    {
        auto initial_size = result.size();

        std::span chunk_view(chunk);
        for (std::size_t i = 0; i < data.size(); i += chunk_view.size_bytes())
        {
            auto written_values = importChunk(data.subspan(i), chunk_view);
            encodeChunk(chunk_view.subspan(0, written_values));
        }

        return initial_size - result.size();
    }

    void decode(std::span<const std::byte> values, std::size_t decoded_size) &&
    {
        std::size_t read_bytes{0};

        std::span<TUint> chunk_view(chunk);
        for (std::size_t i = 0; i < decoded_size; i += chunk_view.size_bytes())
        {
            if (i + chunk_view.size_bytes() > decoded_size)
                chunk_view = chunk_view.first(ceilBytesToEvenValues(decoded_size - i));
            read_bytes += decodeChunk(values.subspan(read_bytes), chunk_view);
            exportChunk(chunk_view);
        }
    }

private:
    static std::size_t ceilBytesToEvenValues(std::size_t bytes_count)
    {
        auto values_count = (bytes_count + VALUE_SIZE - 1) / VALUE_SIZE;
        return values_count % 2 == 0 ? values_count : values_count + 1;
    }

    std::size_t importChunk(std::span<const std::byte> values, std::span<TUint> chnk)
    {
        if (auto chunk_view = std::as_writable_bytes(chnk); chunk_view.size() <= values.size())
        {
            std::memcpy(chunk_view.data(), values.data(), chunk_view.size());
            return chunk_view.size() / VALUE_SIZE;
        }
        else
        {
            std::memset(chunk_view.data(), 0, chunk_view.size());
            std::memcpy(chunk_view.data(), values.data(), values.size());
            return ceilBytesToEvenValues(values.size());
        }
    }

    void exportChunk(std::span<const TUint> chnk)
    {
        auto chunk_view = std::as_bytes(chnk).first(std::min(result.size(), chnk.size_bytes()));
        std::memcpy(result.data(), chunk_view.data(), chunk_view.size());
        result = result.subspan(chunk_view.size());
    }

    void encodeChunk(std::span<const TUint> seq)
    {
        for (std::size_t i = 0; i < seq.size(); i += 2)
        {
            encodePair(seq[i], seq[i + 1]);
        }
    }

    struct CompressedValue
    {
        TUint value;
        unsigned compressed_size;
        bool is_dfcm_predictor;
    };

    unsigned encodeCompressedSize(int compressed)
    {
        if constexpr (VALUE_SIZE > MAX_COMPRESSED_SIZE)
        {
            if (compressed >= 4)
                --compressed;
        }
        return std::min(static_cast<unsigned>(compressed), MAX_COMPRESSED_SIZE);
    }

    unsigned decodeCompressedSize(unsigned encoded_size)
    {
        if constexpr (VALUE_SIZE > MAX_COMPRESSED_SIZE)
        {
            if (encoded_size > 3)
                ++encoded_size;
        }
        return encoded_size;
    }

    CompressedValue compressValue(TUint value) noexcept
    {
        TUint compressed_dfcm = dfcm_predictor.predict() ^ value;
        TUint compressed_fcm = fcm_predictor.predict() ^ value;
        dfcm_predictor.add(value);
        fcm_predictor.add(value);
        auto zeroes_dfcm = std::countl_zero(compressed_dfcm);
        auto zeroes_fcm = std::countl_zero(compressed_fcm);
        if (zeroes_dfcm > zeroes_fcm)
            return {compressed_dfcm, encodeCompressedSize(zeroes_dfcm / CHAR_BIT), true};
        return {compressed_fcm, encodeCompressedSize(zeroes_fcm / CHAR_BIT), false};
    }

    void encodePair(TUint first, TUint second)
    {
        auto [value1, compressed_size1, is_dfcm_predictor1] = compressValue(first);
        auto [value2, compressed_size2, is_dfcm_predictor2] = compressValue(second);
        std::byte header{0x0};
        if (is_dfcm_predictor1)
            header |= DFCM_BIT_1;
        if (is_dfcm_predictor2)
            header |= DFCM_BIT_2;
        header |= static_cast<std::byte>((compressed_size1 << 4) | compressed_size2);
        result.front() = header;

        compressed_size1 = decodeCompressedSize(compressed_size1);
        compressed_size2 = decodeCompressedSize(compressed_size2);
        auto tail_size1 = VALUE_SIZE - compressed_size1;
        auto tail_size2 = VALUE_SIZE - compressed_size2;

        std::memcpy(result.data() + 1, valueTail(value1, compressed_size1), tail_size1);
        std::memcpy(result.data() + 1 + tail_size1, valueTail(value2, compressed_size2), tail_size2);
        result = result.subspan(1 + tail_size1 + tail_size2);
    }

    std::size_t decodeChunk(std::span<const std::byte> values, std::span<TUint> seq)
    {
        std::size_t read_bytes{0};
        for (std::size_t i = 0; i < seq.size(); i += 2)
        {
            read_bytes += decodePair(values.subspan(read_bytes), seq[i], seq[i + 1]);
        }
        return read_bytes;
    }

    TUint decompressValue(TUint value, bool isDfcmPredictor)
    {
        TUint decompressed;
        if (isDfcmPredictor)
        {
            decompressed = dfcm_predictor.predict() ^ value;
        }
        else
        {
            decompressed = fcm_predictor.predict() ^ value;
        }
        dfcm_predictor.add(decompressed);
        fcm_predictor.add(decompressed);
        return decompressed;
    }

    std::size_t decodePair(std::span<const std::byte> bytes, TUint& first, TUint& second)
    {
        if (bytes.empty())
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Unexpected end of encoded sequence");

        auto compressed_size1 = decodeCompressedSize(static_cast<unsigned>(bytes.front() >> 4) & MAX_COMPRESSED_SIZE);
        auto compressed_size2 = decodeCompressedSize(static_cast<unsigned>(bytes.front()) & MAX_COMPRESSED_SIZE);

        auto tail_size1 = VALUE_SIZE - compressed_size1;
        auto tail_size2 = VALUE_SIZE - compressed_size2;

        if (bytes.size() < 1 + tail_size1 + tail_size2)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Unexpected end of encoded sequence");

        TUint value1{0};
        TUint value2{0};

        std::memcpy(valueTail(value1, compressed_size1), bytes.data() + 1, tail_size1);
        std::memcpy(valueTail(value2, compressed_size2), bytes.data() + 1 + tail_size1, tail_size2);

        auto is_dfcm_predictor1 = static_cast<unsigned char>(bytes.front() & DFCM_BIT_1);
        auto is_dfcm_predictor2 = static_cast<unsigned char>(bytes.front() & DFCM_BIT_2);
        first = decompressValue(value1, is_dfcm_predictor1 != 0);
        second = decompressValue(value2, is_dfcm_predictor2 != 0);

        return 1 + tail_size1 + tail_size2;
    }

    static void* valueTail(TUint& value, unsigned compressed_size)
    {
        if constexpr (Endian == std::endian::little)
        {
            return &value;
        }
        else
        {
            return reinterpret_cast<std::byte*>(&value) + compressed_size;
        }
    }

    DfcmPredictor<TUint> dfcm_predictor;
    FcmPredictor<TUint> fcm_predictor;
    std::array<TUint, CHUNK_SIZE> chunk{};
    std::span<std::byte> result{};
};

}

UInt32 CompressionCodecFPC::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    dest[0] = static_cast<char>(float_width);
    dest[1] = static_cast<char>(level);
    dest[2] = static_cast<char>(encodeEndianness(std::endian::native));

    auto dest_size = getMaxCompressedDataSize(source_size);
    auto destination = std::as_writable_bytes(std::span(dest, dest_size).subspan(HEADER_SIZE));
    auto src = std::as_bytes(std::span(source, source_size));
    switch (float_width)
    {
        case sizeof(Float64):
            return HEADER_SIZE + FPCOperation<UInt64>(destination, level).encode(src);
        case sizeof(Float32):
            return HEADER_SIZE + FPCOperation<UInt32>(destination, level).encode(src);
        default:
            break;
    }
    throw Exception("Cannot compress. File has incorrect float width", ErrorCodes::CANNOT_COMPRESS);
}

void CompressionCodecFPC::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < HEADER_SIZE)
        throw Exception("Cannot decompress. File has wrong header", ErrorCodes::CANNOT_DECOMPRESS);

    auto compressed_data = std::span(source, source_size);
    if (decodeEndianness(static_cast<UInt8>(compressed_data[2])) != std::endian::native)
        throw Exception("Cannot decompress. File has incorrect endianness", ErrorCodes::CANNOT_DECOMPRESS);

    auto compressed_float_width = static_cast<UInt8>(compressed_data[0]);
    auto compressed_level = static_cast<UInt8>(compressed_data[1]);
    if (compressed_level == 0)
        throw Exception("Cannot decompress. File has incorrect level", ErrorCodes::CANNOT_DECOMPRESS);

    auto destination = std::as_writable_bytes(std::span(dest, uncompressed_size));
    auto src = std::as_bytes(compressed_data.subspan(HEADER_SIZE));
    switch (compressed_float_width)
    {
        case sizeof(Float64):
            FPCOperation<UInt64>(destination, compressed_level).decode(src, uncompressed_size);
            break;
        case sizeof(Float32):
            FPCOperation<UInt32>(destination, compressed_level).decode(src, uncompressed_size);
            break;
        default:
            break;
    }
    throw Exception("Cannot decompress. File has incorrect float width", ErrorCodes::CANNOT_DECOMPRESS);
}

}
