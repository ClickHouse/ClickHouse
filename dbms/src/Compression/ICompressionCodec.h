#pragma once

#include <memory>
#include <Common/COWPtr.h>
#include <boost/noncopyable.hpp>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>


namespace DB
{

class ICompressionCodec;
using CodecPtr = std::shared_ptr<const ICompressionCodec>;
using Codecs = std::vector<CodecPtr>;


/** Properties of codec.
  * Contains methods for compression / decompression.
  * Implementations of this interface represent a codec (example: None, ZSTD)
  *  or parapetric family of codecs (example: LZ4(...)).
  *
  * Codec is totally immutable object. You can always share them.
  */
class ICompressionCodec : private boost::noncopyable
{
public:
    uint8_t bytecode = 0x0;
    DataTypePtr dataType;

    /// Name of codec (examples: LZ4(...), None).
    virtual String getName() const { return getFamilyName(); };

    /// Name of codec family (example: LZ4, ZSTD).
    virtual const char * getFamilyName() const = 0;

    virtual size_t getArgHeaderSize() const = 0;

    /// Header for serialization, containing bytecode and parameters
    virtual size_t writeHeader(char* header);
    /// Header parser for parameters
    virtual size_t parseHeader(const char* header);
    /// Maximum amount of bytes for compression needed
    virtual size_t getMaxCompressedSize(size_t uncompressed_size) const = 0;
    virtual size_t getMaxDecompressedSize(size_t compressed_size) const = 0;

    /// Block compression and decompression methods
    virtual size_t compress(const PODArray<char>& source, PODArray<char>& dest, int inputSize, int maxOutputSize) const = 0;
    virtual size_t decompress(const PODArray<char>& source, PODArray<char>& dest, int inputSize, int maxOutputSize) const = 0;

    /// Data type information provider
    virtual void setDataType(DataTypePtr data_type) = 0;

    virtual ~ICompressionCodec() {}
};

enum class CodecHeaderBits : UInt8
{
    CONTINUATION_BIT = 0x01
};
}