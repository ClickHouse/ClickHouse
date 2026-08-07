#include <Compression/ICompressionCodec.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Compression/CompressionInfo.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <base/extended_types.h>
#include <base/unaligned.h>

#include <boost/integer/common_factor.hpp>
#include <libdivide-config.h>
#include <libdivide.h>


namespace DB
{

class CompressionCodecGCD : public ICompressionCodec
{
public:
    explicit CompressionCodecGCD(UInt8 gcd_bytes_size_, bool is_signed_type_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:
    /// 1 byte (`gcd_bytes_size` value) + 1 byte (`bytes_to_skip` value) + `bytes_to_skip` bytes (trash) + `gcd_bytes_size` bytes (gcd value) + (`source_size` - `bytes_to_skip`) bytes (data)
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }
    String getDescription() const override
    {
        return "Preprocessor. Greatest common divisor compression; divides values by a common divisor; effective for divisible integer sequences.";
    }

private:
    const UInt8 gcd_bytes_size;
    const bool is_signed_type;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

CompressionCodecGCD::CompressionCodecGCD(UInt8 gcd_bytes_size_, bool is_signed_type_)
    : gcd_bytes_size(gcd_bytes_size_)
    , is_signed_type(is_signed_type_)
{
    setCodecDescription("GCD", {});
}

UInt32 CompressionCodecGCD::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return uncompressed_size
           + gcd_bytes_size // To store gcd
           + 2; // Values of `gcd_bytes_size` and `bytes_to_skip`
}

uint8_t CompressionCodecGCD::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::GCD);
}

void CompressionCodecGCD::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
    hash.update(gcd_bytes_size);
    hash.update(is_signed_type);
}

namespace
{

template <typename T>
void compressDataForType(const char * source, UInt32 source_size, char * dest, bool is_signed)
{
    static_assert(!is_signed_v<T>);
    if (source_size % sizeof(T) != 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with GCD codec, data size {} is not aligned to {}", source_size, sizeof(T));

    const char * const source_end = source + source_size;

    /// Return the unsigned magnitude of a raw value, handling signed wrap-around.
    using S = make_signed_t<T>;
    auto toMagnitude = [is_signed](T val) -> T
    {
        return (is_signed && static_cast<S>(val) < S(0)) ? T(0) - val : val;
    };

    T gcd = 0;
    const auto * cur_source = source;
    while (gcd != T(1) && cur_source < source_end)
    {
        T val = unalignedLoad<T>(cur_source);
        T magnitude = toMagnitude(val);
        gcd = (cur_source == source) ? magnitude : boost::integer::gcd(gcd, magnitude);
        cur_source += sizeof(T);
    }

    unalignedStore<T>(dest, gcd);
    dest += sizeof(T);

    /// GCD compression is pointless if GCD = 1 or GCD = 0 (happens with 0 values in data).
    /// In these cases only copy the source to dest, i.e. don't compress.
    if (gcd == 0 || gcd == 1)
    {
        memcpy(dest, source, source_size);
        return;
    }

    if constexpr (sizeof(T) <= 8)
    {
        /// libdivide supports only UInt32 and UInt64
        using LibdivideT = std::conditional_t<sizeof(T) <= 4, UInt32, UInt64>;
        libdivide::divider<LibdivideT> divider(static_cast<LibdivideT>(gcd));
        cur_source = source;
        while (cur_source < source_end)
        {
            T val = unalignedLoad<T>(cur_source);
            T magnitude = toMagnitude(val);
            T quotient_magnitude = static_cast<T>(static_cast<LibdivideT>(magnitude) / divider);
            T quotient = (is_signed && magnitude != val) ? T(0) - quotient_magnitude : quotient_magnitude;
            unalignedStore<T>(dest, quotient);
            cur_source += sizeof(T);
            dest += sizeof(T);
        }
    }
    else
    {
        cur_source = source;
        while (cur_source < source_end)
        {
            T val = unalignedLoad<T>(cur_source);
            T mag = toMagnitude(val);
            T quotient_magnitude = mag / gcd;
            T quotient = (is_signed && mag != val) ? T(0) - quotient_magnitude : quotient_magnitude;
            unalignedStore<T>(dest, quotient);
            cur_source += sizeof(T);
            dest += sizeof(T);
        }
    }
}

template <typename T>
UInt32 decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    const char * original_dest = dest;
    if (source_size % sizeof(T) != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress GCD-encoded data, data size {} is not aligned to {}", source_size, sizeof(T));

    if (source_size < sizeof(T))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress GCD-encoded data, data size {} is less than {}", source_size, sizeof(T));

    const char * const source_end = source + source_size;
    const char * const dest_end = dest + output_size;

    const T gcd_multiplier = unalignedLoad<T>(source);
    source += sizeof(T);

    /// Handle special cases GCD = 1 and GCD = 0.
    if (gcd_multiplier == 0 || gcd_multiplier == 1)
    {
        /// Subtraction is safe, because we checked that source_size >= sizeof(T)
        if (source_size - sizeof(T) != output_size)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress GCD-encoded data");

        memcpy(dest, source, source_size - sizeof(T));
        return source_size - sizeof(T);
    }

    while (source < source_end)
    {
        if (dest + sizeof(T) > dest_end) [[unlikely]]
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress GCD-encoded data");
        unalignedStore<T>(dest, unalignedLoad<T>(source) * gcd_multiplier);

        source += sizeof(T);
        dest += sizeof(T);
    }
    chassert(source == source_end);
    return static_cast<UInt32>(dest - original_dest);
}

}

UInt32 CompressionCodecGCD::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % gcd_bytes_size;
    dest[0] = gcd_bytes_size;
    dest[1] = bytes_to_skip; /// unused (backward compatibility)
    memcpy(&dest[2], source, bytes_to_skip);
    size_t start_pos = 2 + bytes_to_skip;
    switch (gcd_bytes_size)
    {
    case 1:
        compressDataForType<UInt8>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], is_signed_type);
        break;
    case 2:
        compressDataForType<UInt16>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], is_signed_type);
        break;
    case 4:
        compressDataForType<UInt32>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], is_signed_type);
        break;
    case 8:
        compressDataForType<UInt64>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], is_signed_type);
        break;
    case 16:
        compressDataForType<UInt128>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], is_signed_type);
        break;
    case 32:
        compressDataForType<UInt256>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos], is_signed_type);
        break;
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot compress to GCD-encoded data. Invalid byte size {}", UInt32{gcd_bytes_size});
    }
    return 2 + gcd_bytes_size + source_size;
}

UInt32 CompressionCodecGCD::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress GCD-encoded data. File has wrong header");

    if (uncompressed_size == 0)
        return 0;

    UInt8 bytes_size = source[0];

    if (!(bytes_size == 1 || bytes_size == 2 || bytes_size == 4 || bytes_size == 8 || bytes_size == 16 || bytes_size == 32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress GCD-encoded data. File has wrong header");

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;

    if (bytes_to_skip != static_cast<UInt8>(source[1]))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress GCD-encoded data. File has wrong header");

    UInt32 output_size = uncompressed_size - bytes_to_skip;

    if (static_cast<UInt32>(2 + bytes_to_skip) > source_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress GCD-encoded data. File has wrong header");

    memcpy(dest, &source[2], bytes_to_skip);
    UInt32 source_size_no_header = source_size - bytes_to_skip - 2;
    /// Unsigned types are used for all cases: unsigned modular multiply (mod 2^N) equals signed
    /// multiply, so signed quotients stored during compression reconstruct correctly.
    switch (bytes_size)
    {
    case 1:
        return bytes_to_skip + decompressDataForType<UInt8>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
    case 2:
        return bytes_to_skip + decompressDataForType<UInt16>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
    case 4:
        return bytes_to_skip + decompressDataForType<UInt32>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
    case 8:
        return bytes_to_skip + decompressDataForType<UInt64>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
    case 16:
        return bytes_to_skip + decompressDataForType<UInt128>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
    case 32:
        return bytes_to_skip + decompressDataForType<UInt256>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
    default:
        /// This should be unreachable due to the check above
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot decompress GCD-encoded data. File has unknown byte size {}", UInt32{bytes_size});
    }
}

namespace
{

std::pair<UInt8, bool> getGCDTypeInfo(const IDataType * column_type)
{
    WhichDataType which(column_type);
    if (!(which.isInt() || which.isUInt() || which.isDecimal() || which.isDateOrDate32() || which.isDateTime() ||which.isDateTime64()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec GCD cannot be applied to column {} because it can only be used with Int*, UInt*, Decimal*, Date* or DateTime* types.",
            column_type->getName());

    size_t max_size = column_type->getSizeOfValueInMemory();
    if (max_size == 1 || max_size == 2 || max_size == 4 || max_size == 8 || max_size == 16 || max_size == 32)
        return {static_cast<UInt8>(max_size), !column_type->isValueRepresentedByUnsignedInteger()};
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Codec GCD is only applicable for data types of size 1, 2, 4, 8, 16, 32 bytes. Given type {}",
        column_type->getName());
}

}

void registerCodecGCD(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::GCD);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        /// Default: 1-byte unsigned.
        UInt8 gcd_bytes_size = 1;
        bool is_signed_type = false;

        if (arguments && !arguments->children.empty())
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "GCD codec must have 0 parameters, given {}", arguments->children.size());
        if (column_type)
        {
            std::tie(gcd_bytes_size, is_signed_type) = getGCDTypeInfo(column_type);
        }
        else
        {
            /// Unlike `T64`, `GCD` intentionally does not throw when type information is missing.
            /// This still preserves correctness, but for signed data it can degrade divisor
            /// discovery and therefore compression efficiency.
        }

        return std::make_shared<CompressionCodecGCD>(gcd_bytes_size, is_signed_type);
    };
    factory.registerCompressionCodecWithType("GCD", method_code, codec_builder);
}

CompressionCodecPtr getCompressionCodecGCD(UInt8 gcd_bytes_size)
{
    return std::make_shared<CompressionCodecGCD>(gcd_bytes_size, /*is_signed_type=*/false);
}

}
