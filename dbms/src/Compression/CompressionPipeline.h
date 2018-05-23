#pragma once

#include <memory>
#include <Common/COWPtr.h>
#include <boost/noncopyable.hpp>
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
    std::vector<uint32_t> input_sizes;
    std::vector<uint32_t> output_sizes;
public:
    CompressionPipeline(CodecPtrs codecs)
        : codecs (codecs)
    {
        for (auto & codec: codecs) {
            auto codec_header = codec.getHeader();
            header.insert(header.end(), codec_header.begin(), codec_header.end());
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
    size_t writeHeader(uint8_t* out)
    {
        size_t hs = 0;
        for (; hs < header.size(); ++hs)
            out[hs] = header[hs];
        return hs;
    };

    /// Maximum amount of bytes for compression needed
    size_t getMaxCompressedSize(size_t uncompressed_size)
    {
        size_t max_compressed = uncompressed_size, compressed = uncompressed_size;
        for (auto & codec : codecs) {
            compressed = codec.getMaxCompressedSize(compressed);
            max_compressed = std::max(compressed, max_compressed);
        }
        return max_compressed;
    };

    /// Block compression and decompression methods
    size_t compress(const char* source, char* dest, int inputSize, int maxOutputSize)
    {
        bool first = true, it = true;
        char* tmpdest = new char[maxOutputSize];
        for (auto & codec: codecs) {
            if (first) {
                inputSize = codec.compress(source, dest, inputSize, maxOutputSize);
            }
            else {
                if (it)
                    inputSize = codec.compress(dest, tmpdest, inputSize, maxOutputSize);
                else
                    inputSize = codec.compress(tmpdest, dest, inputSize, maxOutputSize);
            }
        }
        if (it) {
            memcpy();
        }
        return inputSize;
    };
    size_t decompress(void* dest, size_t maxOutputSize, const void* source, size_t inputSize)
    {
        bool first = true;
        auto _dest = dest;
        for (auto & codec : boost::adaptors::reverse(codecs)) {
            dest = _dest;
            if (first) {
                inputSize = codec.decompress(dest, maxOutputSize, source, inputSize);
            }
            else
                inputSize = codec.decompress(dest, maxOutputSize, dest, inputSize);
        }
        return inputSize;
    };

    /// Checks that two instances belong to the same type
    bool equals(const CompressionPipeline & rhs) const = 0;

    ~CompressionPipeline() {}
};

}