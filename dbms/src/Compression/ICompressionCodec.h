#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include <Core/Field.h>
#include <Common/PODArray.h>
#include <DataTypes/IDataType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class ICompressionCodec;

using CodecPtr = std::shared_ptr<ICompressionCodec>;
using Codecs = std::vector<CodecPtr>;


/** Properties of codec.
  * Contains methods for compression / decompression.
  * Implementations of this interface represent a codec (example: None, ZSTD)
  *  or parapetric family of codecs (example: LZ4(...)).
  *
  * Codec is totally immutable object. You can always share them.
  */
class ICompressionCodec : private boost::noncopyable {
public:
    std::vector<uint32_t> data_sizes;
    DataTypePtr data_type;
    static const uint8_t bytecode = 0x0;

    /// Name of codec (examples: LZ4(...), None).
    virtual String getName() const { return getFamilyName(); };

    /// Name of codec family (example: LZ4, ZSTD).
    virtual const char *getFamilyName() const = 0;

    /// Header size of internal header (arguments, excluding bytecode), size parsed by parseHeader
    virtual size_t getHeaderSize() const { return 0; };
    /// Header for serialization, containing bytecode and parameters
    virtual size_t writeHeader(char *) = 0;
    /// Header parser for parameters
    virtual size_t parseHeader(const char *) { return 0; };

    /// Parsed sizes from data block
    virtual size_t getCompressedSize() const = 0;
    virtual size_t getDecompressedSize() const = 0;

    /// Maximum amount of bytes for compression needed
    virtual size_t getMaxCompressedSize(size_t uncompressed_size) const = 0;

    /// Block compression and decompression methods for Pipeline and Codec
    virtual size_t compress(char *, PODArray<char> &, int, int)
    {
        throw Exception("Cannot compress into PODArray from Codec " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }
    virtual size_t compress(char* source, char* dest, int inputSize, int maxOutputSize) = 0;
    virtual size_t decompress(char *source, char *dest, int inputSize, int maxOutputSize) = 0;

    /// Data type information provider
    virtual void setDataType(DataTypePtr _data_type)
    {
        data_type = _data_type;
    };

    virtual ~ICompressionCodec() {}
};

}