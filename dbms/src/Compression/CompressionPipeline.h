#pragma once

#include <common/unaligned.h>
#include <IO/ReadBuffer.h>
#include <Compression/ICompressionCodec.h>
#include <DataTypes/IDataType.h>
#include <Common/PODArray.h>
#include <Parsers/IAST.h>


namespace DB
{
/** Create codecs compression pipeline for sequential compression.
  * For example: CODEC(LZ4, ZSTD)
  */
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using CodecPtr = std::shared_ptr<ICompressionCodec>;
using Codecs = std::vector<CodecPtr>;

class CompressionPipeline;
using PipePtr = std::shared_ptr<CompressionPipeline>;

class CompressionPipeline final : public ICompressionCodec
{
private:
    Codecs codecs;
    /// Sizes of data mutations, from original to later compressions
    std::vector<UInt32> data_sizes;
    size_t header_size = 0;
    DataTypePtr data_type;
public:
    CompressionPipeline(ReadBuffer* header);
    CompressionPipeline(Codecs& _codecs)
        : codecs (_codecs)
    {}

    static PipePtr get_pipe(ReadBuffer* header);
    static PipePtr get_pipe(ASTPtr &);

    String getName() const;

    const char *getFamilyName() const override;
    /// Header for serialization, containing bytecode and parameters
    size_t writeHeader(char* out);
    size_t getHeaderSize() const;

    size_t getCompressedSize() const;
    size_t getDecompressedSize() const;

    /** Maximum amount of bytes for compression needed
     * Returns size of first codec in pipeline as for iterative approach.
     * @param uncompressed_size - data to be compressed in bytes;
     * @return size of maximum buffer for first compression needed.
     */
    size_t getMaxCompressedSize(size_t uncompressed_size) const;
    size_t getMaxDecompressedSize(size_t) const;

    /// Block compression and decompression methods
    size_t compress(char *source, PODArray<char> &dest, int inputSize, int maxOutputSize);
    size_t compress(char*, char*, int, int)
    {
        throw Exception("Could not compress into `char*` from Pipeline", ErrorCodes::NOT_IMPLEMENTED);
    }
    size_t decompress(char* source, char* dest, int inputSize, int) override;

    void setDataType(DataTypePtr _data_type) override;
};

}