#pragma once

#include <memory>
#include <common/unaligned.h>
#include <IO/CompressedStream.h>
#include <IO/ReadBuffer.h>
#include <Compression/CompressionCodecFactory.h>
#include <Compression/ICompressionCodec.h>
#include <DataTypes/IDataType.h>


namespace DB
{
/** Create codecs compression pipeline for sequential compression.
  * For example: CODEC(LZ4, ZSTD)
  */
class ReadBuffer;

using CodecPtr = std::shared_ptr<ICompressionCodec>;
using Codecs = std::vector<CodecPtr>;

class CompressionPipeline;
using PipePtr = std::shared_ptr<CompressionPipeline>;

class CompressionPipeline
{
private:
    Codecs codecs;
    /// Sizes of data mutations, from original to later compressions
    std::vector<uint32_t> data_sizes;
    size_t header_size = 0;
    DataTypePtr data_type;
public:
    CompressionPipeline();

    CompressionPipeline(Codecs _codecs)
        : codecs (_codecs)
    {}

    CompressionPipeline(ReadBuffer *& header);

    size_t getHeaderSize() const
    {
        return header_size;
    }

    String getName() const
    {
        String name("CODEC(");
        bool first = true;
        for (auto & codec: codecs)
            name += first ? codec->getName() : ", " + codec->getName();
        name += ")";
        return name;
    }

    const char *getFamilyName() const
    {
        return "CODEC";
    }

    /// Header for serialization, containing bytecode and parameters
    size_t writeHeader(char* out)
    {
        size_t wrote_size = 0;
        for (int i = codecs.size() - 1; i >= 0; --i)
        {
            auto wrote = codecs[i]->writeHeader(out);
            *out |= i ? static_cast<char>(CompressionMethodByte::CONT_BIT) : 0;
            out += wrote;
            wrote_size += wrote;
        }
        for (int i = data_sizes.size() - 1; i >= 0; --i)
        {
            unalignedStore(&out[sizeof(uint32_t) * i], data_sizes[i]);
            wrote_size += sizeof(uint32_t) * i;
        }
        return wrote_size;
    }

    size_t getCompressedSize() const
    {
        return data_sizes.back();
    }

    size_t getDecompressedSize() const
    {
        return data_sizes.front();
    }

    /** Maximum amount of bytes for compression needed
     * Returns size of first codec in pipeline as for iterative approach.
     * @param uncompressed_size - data to be compressed in bytes;
     * @return size of maximum buffer for first compression needed.
     */
    size_t getMaxCompressedSize(size_t uncompressed_size) const
    {
        return codecs[0]->getMaxCompressedSize(uncompressed_size);
    }

    size_t getMaxDecompressedSize(size_t) const
    {
        return data_sizes.front();
    }

    /// Block compression and decompression methods
    size_t compress(char* source, PODArray<char>& dest, int inputSize, int maxOutputSize)
    {
        PODArray<char> buffer;
        data_sizes.resize(1);
        data_sizes[0] = inputSize;

        PODArray<char> *_source = reinterpret_cast<PODArray<char>*>(source), *_dest = &dest;
        for (size_t i = 0; i < codecs.size(); ++i) {
            (*_dest).resize(maxOutputSize);
            inputSize = codecs[i]->compress(&(*_source)[0], *_dest, inputSize, maxOutputSize);
            data_sizes.push_back(inputSize);

            maxOutputSize = i + 1 < codecs.size() ? codecs[i + 1]->getMaxCompressedSize(inputSize) : inputSize;
            _source = _dest;
            _dest = *_dest == dest ? &buffer : &dest;
        }

        if (_dest == &dest) {
            dest.assign(buffer);
        }
        return inputSize;
    }

    size_t decompress(char* source, char* dest, int inputSize, int maxOutputSize)
    {
        assert (codecs.size() + 1 == data_sizes.size()); /// All mid sizes should be presented

        PODArray<char> buffer;
        PODArray<char> *_source = reinterpret_cast<PODArray<char>*>(source), *_dest = reinterpret_cast<PODArray<char>*>(dest);
        for (int i = codecs.size() - 1; i >= 0; --i) {
            (*_dest).resize(maxOutputSize);
            inputSize = codecs[i]->decompress(&(*_source)[0], &(*_dest)[0], inputSize, maxOutputSize);
            maxOutputSize = data_sizes[i];

            _source = _dest;
            _dest = (_dest == reinterpret_cast<PODArray<char>*>(dest)) ? &buffer : reinterpret_cast<PODArray<char>*>(dest);
        }

        if (_dest == reinterpret_cast<PODArray<char>*>(dest)) {
            memcpy(dest, &buffer[0], maxOutputSize);
        }
        return inputSize;
    }

    void setDataType(DataTypePtr _data_type)
    {
        data_type = _data_type;
        for (auto & codec: codecs)
        {
            codec->setDataType(data_type);
        }
    }

    static PipePtr get_pipe(ReadBuffer*& header);
    static PipePtr get_pipe(String &);
    static PipePtr get_pipe(ASTPtr &);

    ~CompressionPipeline() {}
};

}