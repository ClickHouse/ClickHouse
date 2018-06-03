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

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;
using Codecs = std::vector<CompressionCodecPtr>;

class CompressionPipeline;
using CompressionPipelinePtr = std::shared_ptr<CompressionPipeline>;

class CompressionPipeline final : public ICompressionCodec
{
private:
    Codecs codecs;
    /// Sizes of data mutations, from original to later compressions
    std::vector<UInt32> data_sizes;
    DataTypePtr data_type = nullptr;
    size_t header_size = 0;
public:
    ASTPtr codec_ptr = nullptr;
    CompressionPipeline(ReadBuffer * header);
    CompressionPipeline(Codecs & codecs_)
        : codecs (codecs_)
    {}

    static CompressionPipelinePtr createPipelineFromBuffer(ReadBuffer *);
    static CompressionPipelinePtr createPipelineFromString(const String &);
    static CompressionPipelinePtr createPipelineFromASTPtr(ASTPtr &);

    String getName() const;

    const char *getFamilyName() const override;
    /// Header for serialization, containing bytecode and parameters
    size_t writeHeader(char * out, std::vector<uint32_t> & ds);
    size_t writeHeader(char *) override
    {
        throw Exception("Not provided data sizes for compression pipeline header", ErrorCodes::LOGICAL_ERROR);
    }
    size_t getHeaderSize() const;

    size_t getCompressedSize() const;
    size_t getDecompressedSize() const;

    /** Maximum amount of bytes for compression needed
     * Returns size of first codec in pipeline as for iterative approach.
     * @param uncompressed_size - data to be compressed in bytes;
     * @return size of maximum buffer for first compression needed.
     */
    size_t getMaxCompressedSize(size_t uncompressed_size) const override;
    size_t getMaxDecompressedSize(size_t) const;

    /// Block compression and decompression methods
    size_t compress(char * source, PODArray<char> & dest, size_t input_size, size_t max_output_size);
    size_t compress(char *, char *, size_t, size_t)
    {
        throw Exception("Could not compress into `char*` from Pipeline", ErrorCodes::NOT_IMPLEMENTED);
    }
    size_t decompress(char *source, char *dest, size_t input_size, size_t) override;

    std::vector<UInt32> getDataSizes() const;

    void setDataType(DataTypePtr _data_type) override;
};

}