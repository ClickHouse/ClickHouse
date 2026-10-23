#include <Compression/CompressionCodecGDeflate.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/registerCompressionCodecs.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Common/PODArray.h>
#include <base/unaligned.h>

#include <gdeflate.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

namespace
{

constexpr size_t chunk_bytes = CompressionCodecGDeflate::chunk_bytes;
constexpr size_t chunk_alignment = 4;

/// Set in the number of chunks of a block whose chunks have packed descriptors.
constexpr UInt32 packed_flag = UInt32{1} << 31;

/// The most bits an integer is packed in, which a read of 8 bytes holds at any bit offset.
constexpr UInt32 max_packed_bits = 56;

/// What `unpack` may read past the packed bits.
constexpr size_t packed_padding = sizeof(UInt64);

size_t alignUp(size_t bytes)
{
    return (bytes + chunk_alignment - 1) / chunk_alignment * chunk_alignment;
}

size_t numChunks(size_t uncompressed_bytes)
{
    return (uncompressed_bytes + chunk_bytes - 1) / chunk_bytes;
}

/// The most a GDeflate page of `chunk_bytes` compresses to.
size_t maxPageBytes()
{
    return libdeflate_gdeflate_compress_bound(nullptr, chunk_bytes, nullptr);
}

bool isPackedElementSize(size_t size)
{
    return size == 1 || size == 2 || size == 4 || size == 8;
}

size_t packedBytes(size_t count, UInt32 bits)
{
    return (count * bits + 7) / 8;
}

struct CompressorDeleter
{
    void operator()(libdeflate_gdeflate_compressor * compressor) const { libdeflate_free_gdeflate_compressor(compressor); }
};

struct DecompressorDeleter
{
    void operator()(libdeflate_gdeflate_decompressor * decompressor) const
    {
        libdeflate_free_gdeflate_decompressor(decompressor);
    }
};

class Chunk
{
public:
    explicit Chunk(bool packed_descriptor_) : packed_descriptor(packed_descriptor_) {}

    static size_t descriptorBytes(bool packed_descriptor) { return packed_descriptor ? 16 : sizeof(UInt32); }

    static size_t maxCompressedBytes() { return alignUp(maxPageBytes()); }

    size_t compressedBytes() const { return alignUp(compressed_size); }

    void compress(
        libdeflate_gdeflate_compressor * compressor,
        const char * source,
        size_t size,
        UInt8 element_size_,
        bool is_signed,
        char * out,
        char * packed,
        char * compressed_packed)
    {
        compressed_size = static_cast<UInt32>(compressPage(compressor, source, size, out));
        if (packed_descriptor)
        {
            element_size = element_size_;
            pack(source, size, is_signed, packed);
            if (bits)
            {
                const size_t packed_size = compressPage(compressor, packed, packedBytes(size / element_size, bits), compressed_packed);
                if (packed_size < compressed_size)
                {
                    memcpy(out, compressed_packed, packed_size);
                    compressed_size = static_cast<UInt32>(packed_size);
                }
                else
                {
                    bits = 0;
                    reference = 0;
                }
            }
        }
        memset(out + compressed_size, 0, compressedBytes() - compressed_size);
    }

    void decompress(const char * in, size_t size, char * out, PODArray<char> & unpacked) const
    {
        if (bits == 0)
        {
            decompressPage(in, out, size);
            return;
        }

        if (bits > max_packed_bits || !isPackedElementSize(element_size) || size % element_size != 0)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "A GDeflate chunk packs integers of {} bytes in {} bits",
                static_cast<UInt32>(element_size),
                static_cast<UInt32>(bits));

        const size_t count = size / element_size;
        const size_t packed_size = packedBytes(count, bits);
        unpacked.resize(packed_size + packed_padding);
        memset(unpacked.data() + packed_size, 0, packed_padding);
        decompressPage(in, unpacked.data(), packed_size);
        unpack(unpacked.data(), count, out);
    }

    void serialize(char * out) const
    {
        unalignedStoreLittleEndian<UInt32>(out, compressed_size);
        if (!packed_descriptor)
            return;
        unalignedStoreLittleEndian<UInt8>(out + 4, bits);
        unalignedStoreLittleEndian<UInt8>(out + 5, element_size);
        unalignedStoreLittleEndian<UInt16>(out + 6, 0);
        unalignedStoreLittleEndian<UInt64>(out + 8, reference);
    }

    void deserialize(const char * in)
    {
        compressed_size = unalignedLoadLittleEndian<UInt32>(in);
        if (!packed_descriptor)
            return;
        bits = unalignedLoadLittleEndian<UInt8>(in + 4);
        element_size = unalignedLoadLittleEndian<UInt8>(in + 5);
        reference = unalignedLoadLittleEndian<UInt64>(in + 8);
    }

    UInt32 compressed_size = 0;

private:
    bool packed_descriptor;
    UInt8 bits = 0;
    UInt8 element_size = 0;
    UInt64 reference = 0;

    template <typename U>
    void packIntegers(const char * source, size_t size, bool is_signed, char * out)
    {
        const U sign_bit = is_signed ? static_cast<U>(U{1} << (8 * sizeof(U) - 1)) : U{0};
        const size_t count = size / sizeof(U);
        U least = unalignedLoadLittleEndian<U>(source) ^ sign_bit;
        U most = least;
        for (size_t i = 1; i < count; ++i)
        {
            const U value = unalignedLoadLittleEndian<U>(source + i * sizeof(U)) ^ sign_bit;
            least = std::min(least, value);
            most = std::max(most, value);
        }

        const auto width = std::max<UInt32>(1, static_cast<UInt32>(std::bit_width(static_cast<UInt64>(static_cast<U>(most - least)))));
        if (width > max_packed_bits || width >= 8 * sizeof(U))
            return;

        /// Each integer is ORed into the bytes from the lowest bit of the byte its first bit is in, 8 bytes at a time, which
        /// hold any 56 bits from any bit offset.
        memset(out, 0, packedBytes(count, width) + packed_padding);
        for (size_t i = 0; i < count; ++i)
        {
            const U value = unalignedLoadLittleEndian<U>(source + i * sizeof(U)) ^ sign_bit;
            const UInt64 delta = static_cast<U>(value - least);
            const size_t first_bit = i * width;
            char * const at = out + first_bit / 8;
            unalignedStoreLittleEndian<UInt64>(at, unalignedLoadLittleEndian<UInt64>(at) | (delta << (first_bit % 8)));
        }
        bits = static_cast<UInt8>(width);
        reference = static_cast<U>(least ^ sign_bit);
    }

    /// Packs the chunk into `out`, of `packedBytes` and `packed_padding` more, if its integers take fewer bits than they are
    /// wide; otherwise leaves `bits` 0.
    void pack(const char * source, size_t size, bool is_signed, char * out)
    {
        switch (element_size)
        {
            case 1: packIntegers<UInt8>(source, size, is_signed, out); break;
            case 2: packIntegers<UInt16>(source, size, is_signed, out); break;
            case 4: packIntegers<UInt32>(source, size, is_signed, out); break;
            default: packIntegers<UInt64>(source, size, is_signed, out); break;
        }
    }

    template <typename U>
    void unpackIntegers(const char * packed, size_t count, char * out) const
    {
        const UInt64 mask = (UInt64{1} << bits) - 1;
        for (size_t i = 0; i < count; ++i)
        {
            const size_t first_bit = i * bits;
            const UInt64 delta = (unalignedLoadLittleEndian<UInt64>(packed + first_bit / 8) >> (first_bit % 8)) & mask;
            unalignedStoreLittleEndian<U>(out + i * sizeof(U), static_cast<U>(reference + delta));
        }
    }

    /// Writes the `count` integers packed at `packed`, readable for `packed_padding` bytes past them.
    void unpack(const char * packed, size_t count, char * out) const
    {
        switch (element_size)
        {
            case 1: unpackIntegers<UInt8>(packed, count, out); break;
            case 2: unpackIntegers<UInt16>(packed, count, out); break;
            case 4: unpackIntegers<UInt32>(packed, count, out); break;
            default: unpackIntegers<UInt64>(packed, count, out); break;
        }
    }

    /// Compresses one page into `out`, of `maxPageBytes`, and returns its compressed size.
    static size_t compressPage(libdeflate_gdeflate_compressor * compressor, const char * page_data, size_t page_size, char * out)
    {
        libdeflate_gdeflate_out_page page{.data = out, .nbytes = maxPageBytes()};
        if (libdeflate_gdeflate_compress(compressor, page_data, page_size, &page, 1) == 0)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress a chunk of {} bytes with GDeflate", page_size);
        return page.nbytes;
    }

    void decompressPage(const char * in, char * out, size_t out_size) const
    {
        thread_local std::unique_ptr<libdeflate_gdeflate_decompressor, DecompressorDeleter> decompressor;
        if (!decompressor)
        {
            decompressor.reset(libdeflate_alloc_gdeflate_decompressor());
            if (!decompressor)
                throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot allocate a GDeflate decompressor");
        }

        libdeflate_gdeflate_in_page page{.data = in, .nbytes = compressed_size};
        size_t decompressed_size = 0;
        const auto result = libdeflate_gdeflate_decompress(decompressor.get(), &page, 1, out, out_size, &decompressed_size);
        if (result != LIBDEFLATE_SUCCESS)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress a GDeflate chunk: error code {}", static_cast<int>(result));
        if (decompressed_size != out_size)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate chunk expands to {} bytes, where {} were expected", decompressed_size, out_size);
    }
};

class Header
{
public:
    Header() = default;
    Header(size_t num_chunks_, bool packed_) : num_chunks(num_chunks_), packed(packed_) {}

    static size_t maxCompressedBytes(size_t num_chunks) { return descriptorOffset(num_chunks, true); }

    size_t compressedBytes() const { return descriptorOffset(num_chunks, packed); }
    size_t descriptorOffset(size_t chunk) const { return descriptorOffset(chunk, packed); }

    void serialize(char * out) const
    {
        unalignedStoreLittleEndian<UInt32>(out, static_cast<UInt32>(num_chunks) | (packed ? packed_flag : 0));
    }

    void deserialize(const char * in)
    {
        const UInt32 word = unalignedLoadLittleEndian<UInt32>(in);
        packed = word & packed_flag;
        num_chunks = word & ~packed_flag;
    }

    size_t num_chunks = 0;
    bool packed = false;

private:
    static size_t descriptorOffset(size_t chunk, bool packed) { return sizeof(UInt32) + chunk * Chunk::descriptorBytes(packed); }
};

}

CompressionCodecGDeflate::CompressionCodecGDeflate(int level_, UInt8 element_size_, bool is_signed_)
    : level(level_)
    , element_size(element_size_)
    , is_signed(is_signed_)
{
}

uint8_t CompressionCodecGDeflate::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::GDeflate);
}

ASTPtr CompressionCodecGDeflate::getCodecDescription() const
{
    return makeCodecDescription("GDeflate", {make_intrusive<ASTLiteral>(static_cast<UInt64>(level))});
}

void CompressionCodecGDeflate::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecGDeflate::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    const size_t num_chunks = numChunks(uncompressed_size);
    return static_cast<UInt32>(Header::maxCompressedBytes(num_chunks) + num_chunks * Chunk::maxCompressedBytes());
}

UInt32 CompressionCodecGDeflate::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    /// A block that ends in the middle of an integer is not packed: its chunks do not hold whole integers.
    const Header header(numChunks(source_size), isPackedElementSize(element_size) && source_size % element_size == 0);
    header.serialize(dest);

    std::unique_ptr<libdeflate_gdeflate_compressor, CompressorDeleter> compressor(
        libdeflate_alloc_gdeflate_compressor(level));
    if (!compressor)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot allocate a GDeflate compressor of level {}", level);

    /// The packed form of a chunk and what it compresses to, kept where it is smaller than the plain one.
    PODArray<char> packed;
    PODArray<char> compressed_packed;
    if (header.packed)
    {
        packed.resize(chunk_bytes + packed_padding);
        compressed_packed.resize(maxPageBytes());
    }

    size_t at = header.compressedBytes();
    for (size_t i = 0; i < header.num_chunks; ++i)
    {
        Chunk chunk(header.packed);
        chunk.compress(
            compressor.get(),
            source + i * chunk_bytes,
            std::min(chunk_bytes, source_size - i * chunk_bytes),
            element_size,
            is_signed,
            dest + at,
            packed.data(),
            compressed_packed.data());
        chunk.serialize(dest + header.descriptorOffset(i));
        at += chunk.compressedBytes();
    }
    return static_cast<UInt32>(at);
}

UInt32 CompressionCodecGDeflate::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < sizeof(UInt32))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "A GDeflate block of {} bytes", source_size);

    Header header;
    header.deserialize(source);
    if (header.num_chunks != numChunks(uncompressed_size) || header.compressedBytes() > source_size)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "A GDeflate block of {} bytes holds {} chunks, where {} expanded bytes make {}",
            source_size,
            header.num_chunks,
            uncompressed_size,
            numChunks(uncompressed_size));

    PODArray<char> unpacked;
    size_t at = header.compressedBytes();
    for (size_t i = 0; i < header.num_chunks; ++i)
    {
        Chunk chunk(header.packed);
        chunk.deserialize(source + header.descriptorOffset(i));
        if (at + chunk.compressed_size > source_size)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "Chunk {} of a GDeflate block of {} bytes ends at {}",
                i,
                source_size,
                at + chunk.compressed_size);

        chunk.decompress(source + at, std::min(chunk_bytes, uncompressed_size - i * chunk_bytes), dest + i * chunk_bytes, unpacked);
        at += chunk.compressedBytes();
    }
    return uncompressed_size;
}

namespace
{

/// The integers whose chunks the codec packs: their size and whether they are signed, or 0 for a column of anything else.
std::pair<UInt8, bool> getPackedTypeInfo(const IDataType * column_type)
{
    if (!column_type)
        return {0, false};

    WhichDataType which(column_type);
    if (!(which.isInt() || which.isUInt() || which.isDecimal() || which.isDateOrDate32() || which.isDateTime() || which.isDateTime64()))
        return {0, false};

    const size_t size = column_type->getSizeOfValueInMemory();
    if (isPackedElementSize(size))
        return {static_cast<UInt8>(size), !column_type->isValueRepresentedByUnsignedInteger()};
    return {0, false};
}

}

void registerCodecGDeflate(CompressionCodecFactory & factory)
{
    const UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::GDeflate);
    factory.registerCompressionCodecWithType("GDeflate", method_code, [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        int level = CompressionCodecGDeflate::DEFAULT_LEVEL;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() != 1)
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "GDeflate codec must have 1 parameter, given {}", arguments->children.size());

            const auto * literal = arguments->children[0]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::UInt64)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "GDeflate codec argument must be a positive integer");

            const UInt64 value = literal->value.safeGet<UInt64>();
            if (value < static_cast<UInt64>(CompressionCodecGDeflate::MIN_LEVEL) || value > static_cast<UInt64>(CompressionCodecGDeflate::MAX_LEVEL))
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "GDeflate codec level must be between {} and {}, given {}",
                    CompressionCodecGDeflate::MIN_LEVEL,
                    CompressionCodecGDeflate::MAX_LEVEL,
                    value);
            level = static_cast<int>(value);
        }

        const auto [element_size, is_signed] = getPackedTypeInfo(column_type);
        return std::make_shared<CompressionCodecGDeflate>(level, element_size, is_signed);
    });
}

}
