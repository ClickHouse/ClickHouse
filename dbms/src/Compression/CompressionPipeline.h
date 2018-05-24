#pragma once

#include <memory>
#include <Common/COWPtr.h>
#include <boost/noncopyable.hpp>
#include <common/unaligned.h>
#include <Core/Field.h>
#include <Compression/ICompressionCodec.h>


namespace DB
{
/** Create codecs compression pipeline for sequential compression.
  * For example: CODEC(LZ4, ZSTD)
  */
class CompressionPipeline : ICompressionCodec {
private:
    CodecPtrs codecs;
    /// Sizes of data mutations, from original to later compressions
    std::vector<uint32_t> data_sizes;
public:
    CompressionPipeline(CodecPtrs codecs)
        : codecs (codecs)
    {}

    CompressionPipeline(ReadBuffer* header)
    {
        const CompressionCodecFactory & codec_factory = CompressionCodecFactory::instance();
        char last_codec_bytecode;
        PODArray<char> _header;
        _header.size(1);
        header->readStrict(&_header[0], 1);
        /// Read codecs, while continuation bit is set
        do {
            last_codec_bytecode = (_header[0]) & (~CodecHeaderBits::CONTINUATION_BIT);
            CodecPtr _codec = codec_factory.get(last_codec_bytecode);
            _header.resize(_codec.getArgHeaderSize());
            header->readStrict(&_header[0], _codec.getArgHeaderSize());
            auto read_chars = _codec.parseHeader(&_header[0]);
            codecs.push_back(_codec);
            header->readStrict(&_header[0], 1);
        }
        while (last_codec_bytecode & CodecHeaderBits::CONTINUATION_BIT);
        /// Load and reverse sizes part of a header, listed from later codecs to the original size, - see `compress`.
        auto codecs_amount = codecs.size();
        _header.resize(codecs_amount + 1);
        output_sizes.resize(codecs_amount + 1);
        header->readStrict(&_header[0], sizeof(UInt32) * (codecs_amount + 1));
        for (size_t i = 0; i <= codecs_amount; ++i) {
            output_sizes[codecs_amount - i] = unalignedLoad<UInt32>(&_header[sizeof(UInt32) * i]);
        }
    }

    String getName() const
    {
        String name("CODEC(");
        bool first = true;
        for (auto & codec: codecs)
            name += first ? codec.getName() : ", " + codec.getName();
        name += ")";
        return name;
    };

    /// Header for serialization, containing bytecode and parameters
    size_t writeHeader(char* out)
    {
        for (int i = codecs.size() - 1; i >= 0; --i)
        {
            auto wrote = codecs[i].writeHeader(out);
            out |= i ? reinterpret_cast<char>(CodecHeaderBits::CONTINUATION_BIT) : 0;
            out += wrote;
        }
        for (int i = data_sizes.size() - 1; i >= 0; --i)
        {
            unalignedStore(&out[sizeof(uint32_t) * i], data_sizes[i]);
        }
    };

    /** Maximum amount of bytes for compression needed
     * Returns size of first codec in pipeline as for iterative approach.
     * @param uncompressed_size - data to be compressed in bytes;
     * @return size of maximum buffer for first compression needed.
     */
    size_t getMaxCompressedSize(size_t uncompressed_size)
    {
        return codecs[0].getMaxCompressedSize(uncompressed_size);
    };

    size_t getMaxDecompressedSize(size_t compressed_size)
    {
        return data_sizes.back();
    };

    /// Block compression and decompression methods
    size_t compress(const PODArray<char>& source, PODArray<char>& dest, int inputSize, int maxOutputSize)
    {
        bool first = true, it = true;
        PODArray<char> buffer;
        data_sizes.resize(1);
        data_sizes[0] = inputSize;

        PODArray<char>* _source = *source, *_dest = *dest;
        for (int i = 0; i < codecs.size(); ++i) {
            (*_dest).resize(maxOutputSize);
            inputSize = codecs[i].compress(_source, _dest, inputSize, maxOutputSize);
            data_sizes.push_back(inputSize);

            maxOutputSize = i + 1 < codecs.size() ? codecs[i + 1].getMaxCompressedSize(inputSize) : inputSize;
            _source = _dest;
            _dest = *_dest == dest ? *buffer : *dest;
        }

        if (_dest == *dest) {
            dest.assign(buffer);
        }
        return inputSize;
    };

    size_t decompress(const PODArray<char>& source, PODArray<char>& dest, size_t inputSize, size_t maxOutputSize)
    {
        assert (codecs.size() + 1 == data_sizes.size()); /// All mid sizes should be presented

        PODArray<char> buffer;
        PODArray<char> *_source = *source, *_dest = *dest;
        for (int i = codecs.size() - 1; i >= 0; --i) {
            (*_dest).resize(maxOutputSize);
            inputSize = codecs[i].decompress(source, dest, inputSize, maxOutputSize);
            maxOutputSize = data_sizes[i];

            _source = _dest;
            _dest = (_dest == *dest) ? *buffer : *dest;
        }

        if (_dest == *dest) {
            dest.assign(buffer);
        }
        return inputSize;
    };

    void setDataType(DataTypePtr data_type)
    {
        data_type = data_type;
        for (auto & codec: codecs)
        {
            codec.setDataType(data_type);
        }
    }

    ~CompressionPipeline() {}
};

}