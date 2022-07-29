#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include <Compression/CompressionInfo.h>
#include <base/types.h>
#include <Parsers/IAST.h>
#include <Common/SipHash.h>


namespace DB
{

class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;
using Codecs = std::vector<CompressionCodecPtr>;

class IDataType;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

/**
* Represents interface for compression codecs like LZ4, ZSTD, etc.
*/
class ICompressionCodec : private boost::noncopyable
{
public:
    virtual ~ICompressionCodec() = default;

    /// Byte which indicates codec in compressed file
    virtual uint8_t getMethodByte() const = 0;

    /// Codec description, for example "ZSTD(2)" or "LZ4,LZ4HC(5)"
    virtual ASTPtr getCodecDesc() const;

    /// Codec description with "CODEC" prefix, for example "CODEC(ZSTD(2))" or
    /// "CODEC(LZ4,LZ4HC(5))"
    ASTPtr getFullCodecDesc() const;

    /// Hash, that depends on codec ast and optional parameters like data type
    virtual void updateHash(SipHash & hash) const = 0;
    UInt64 getHash() const;

    /// Compressed bytes from uncompressed source to dest. Dest should preallocate memory
    UInt32 compress(const char * source, UInt32 source_size, char * dest) const;

    /// Decompress bytes from compressed source to dest. Dest should preallocate memory
    UInt32 decompress(const char * source, UInt32 source_size, char * dest) const;

    /// Number of bytes, that will be used to compress uncompressed_size bytes with current codec
    virtual UInt32 getCompressedReserveSize(UInt32 uncompressed_size) const
    {
        return getHeaderSize() + getMaxCompressedDataSize(uncompressed_size);
    }

    /// Some codecs (LZ4, for example) require additional bytes at end of buffer
    virtual UInt32 getAdditionalSizeAtTheEndOfBuffer() const { return 0; }

    /// Size of header in compressed data on disk
    static constexpr UInt8 getHeaderSize() { return COMPRESSED_BLOCK_HEADER_SIZE; }

    /// Read size of compressed block from compressed source
    static UInt32 readCompressedBlockSize(const char * source);

    /// Read size of decompressed block from compressed source
    static UInt32 readDecompressedBlockSize(const char * source);

    /// Read method byte from compressed source
    static uint8_t readMethod(const char * source);

    /// Return true if this codec actually compressing something. Otherwise it can be just transformation that helps compression (e.g. Delta).
    virtual bool isCompression() const = 0;

    /// Is it a generic compression algorithm like lz4, zstd. Usually it does not make sense to apply generic compression more than single time.
    virtual bool isGenericCompression() const = 0;

    /// If it is a post-processing codec such as encryption. Usually it does not make sense to apply non-post-processing codecs after this.
    virtual bool isEncryption() const { return false; }

    /// It is a codec available only for evaluation purposes and not meant to be used in production.
    /// It will not be allowed to use unless the user will turn off the safety switch.
    virtual bool isExperimental() const { return false; }

    /// If it does nothing.
    virtual bool isNone() const { return false; }

protected:
    /// This is used for fuzz testing
    friend int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

    /// Return size of compressed data without header
    virtual UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const { return uncompressed_size; }

    /// Actually compress data, without header
    virtual UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const = 0;

    /// Actually decompress data without header
    virtual void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const = 0;

    /// Construct and set codec description from codec name and arguments. Must be called in codec constructor.
    void setCodecDescription(const String & name, const ASTList & arguments = {});

private:
    ASTPtr full_codec_desc;
};

}
