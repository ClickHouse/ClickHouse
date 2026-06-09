#include <iomanip>
#include <iostream>

#include <base/types.h>
#include <lz4.h>

#include <IO/ReadBuffer.h>
#include <IO/MMapReadBufferFromFile.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Compression/CompressionInfo.h>
#include <IO/WriteHelpers.h>
#include <Compression/LZ4_decompress_faster.h>
#include <Common/PODArray.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <base/unaligned.h>


/** for i in *.bin; do ./decompress_perf < $i > /dev/null; done
  */

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_COMPRESSION_METHOD;
    extern const int TOO_LARGE_SIZE_COMPRESSED;
    extern const int CANNOT_DECOMPRESS;
}

class FasterCompressedReadBufferBase
{
protected:
    ReadBuffer * compressed_in;

    /// If 'compressed_in' buffer has whole compressed block - then use it. Otherwise copy parts of data to 'own_compressed_buffer'.
    PODArray<char> own_compressed_buffer;
    /// Points to memory, holding compressed block.
    char * compressed_buffer = nullptr;

    ssize_t variant;

    /// Variant for reference implementation of LZ4.
    static constexpr ssize_t LZ4_REFERENCE = -3;

    LZ4::PerformanceStatistics perf_stat;

    size_t readCompressedData(size_t & size_decompressed, size_t & size_compressed_without_checksum)
    {
        if (compressed_in->eof())
            return 0;

        CityHash_v1_0_2::uint128 checksum;
        compressed_in->readStrict(reinterpret_cast<char *>(&checksum), sizeof(checksum));

        own_compressed_buffer.resize(COMPRESSED_BLOCK_HEADER_SIZE);
        compressed_in->readStrict(own_compressed_buffer.data(), COMPRESSED_BLOCK_HEADER_SIZE);

        UInt8 method = own_compressed_buffer[0];    /// See CompressedWriteBuffer.h

        size_t & size_compressed = size_compressed_without_checksum;

        if (method == static_cast<UInt8>(CompressionMethodByte::LZ4) ||
            method == static_cast<UInt8>(CompressionMethodByte::ZSTD) ||
            method == static_cast<UInt8>(CompressionMethodByte::NONE))
        {
            size_compressed = unalignedLoad<UInt32>(&own_compressed_buffer[1]);
            size_decompressed = unalignedLoad<UInt32>(&own_compressed_buffer[5]);
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_COMPRESSION_METHOD, "Unknown compression method: {}", toString(method));

        if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
            throw Exception(ErrorCodes::TOO_LARGE_SIZE_COMPRESSED, "Too large size_compressed. Most likely corrupted data.");

        /// Is whole compressed block located in 'compressed_in' buffer?
        if (compressed_in->offset() >= COMPRESSED_BLOCK_HEADER_SIZE &&
            compressed_in->position() + size_compressed - COMPRESSED_BLOCK_HEADER_SIZE <= compressed_in->buffer().end())
        {
            compressed_in->position() -= COMPRESSED_BLOCK_HEADER_SIZE;
            compressed_buffer = compressed_in->position();
            compressed_in->position() += size_compressed;
        }
        else
        {
            own_compressed_buffer.resize(size_compressed + (variant == LZ4_REFERENCE ? 0 : LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER));
            compressed_buffer = own_compressed_buffer.data();
            compressed_in->readStrict(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, size_compressed - COMPRESSED_BLOCK_HEADER_SIZE);
        }

        return size_compressed + sizeof(checksum);
    }

    void decompress(char * to, size_t size_decompressed, size_t size_compressed_without_checksum)
    {
        UInt8 method = compressed_buffer[0];    /// See CompressedWriteBuffer.h

        // std::cout << "Compressed " << size_compressed_without_checksum;
        // std::cout << " Decompressed " << size_decompressed;
        // std::cout << " Ratio " << static_cast<Float64>(size_compressed_without_checksum) / size_decompressed << std::endl;

        if (method == static_cast<UInt8>(CompressionMethodByte::LZ4))
        {
            //LZ4::statistics(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_decompressed, stat);

            if (variant == LZ4_REFERENCE)
            {
                if (LZ4_decompress_fast(
                    compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to,
                    static_cast<int>(size_decompressed)) < 0)
                {
                    throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot LZ4_decompress_fast");
                }
            }
            else
                LZ4::decompress(compressed_buffer + COMPRESSED_BLOCK_HEADER_SIZE, to, size_compressed_without_checksum, size_decompressed, perf_stat);
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_COMPRESSION_METHOD, "Unknown compression method: {}", toString(method));
    }

public:
    /// 'compressed_in' could be initialized lazily, but before first call of 'readCompressedData'.
    FasterCompressedReadBufferBase(ReadBuffer * in, ssize_t variant_)
        : compressed_in(in), own_compressed_buffer(COMPRESSED_BLOCK_HEADER_SIZE), variant(variant_)
    {
        perf_stat.choose_method = variant;
    }

};


class FasterCompressedReadBuffer : public FasterCompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
{
private:
    size_t size_compressed = 0;

    bool nextImpl() override
    {
        size_t size_decompressed;
        size_t size_compressed_without_checksum;
        size_compressed = readCompressedData(size_decompressed, size_compressed_without_checksum);
        if (!size_compressed)
            return false;

        memory.resize(size_decompressed + LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER);
        working_buffer = Buffer(memory.data(), &memory[size_decompressed]);

        decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

        return true;
    }

public:
    FasterCompressedReadBuffer(ReadBuffer & in_, ssize_t method)
        : FasterCompressedReadBufferBase(&in_, method), BufferWithOwnMemory<ReadBuffer>(0)
    {
    }
};

}


/* Usage example:

#!/bin/sh

./clickhouse-compressor < clickhouse-compressor > compressed
./clickhouse-compressor -d < compressed > clickhouse-compressor2
cmp clickhouse-compressor clickhouse-compressor2 && echo "Ok." || echo "Fail."

*/


int main(int argc, char ** argv)
try
{
    using namespace DB;

    if (argc < 2 || argc > 4)
    {
        std::cerr << "Usage: " << argv[0] << " <file> [times] [variant]\n";
        return 1;
    }
    size_t times = argc < 3 ? 1 : parse<size_t>(argv[2]);
    ssize_t variant = argc < 4 ? -1 : parse<ssize_t>(argv[3]);

    std::vector<UInt64> runs;

    for (size_t i = 0; i < times; i++)
    {
        MMapReadBufferFromFile in(argv[1], 0);
        FasterCompressedReadBuffer decompressing_in(in, variant);

        Stopwatch watch;
        while (!decompressing_in.eof())
        {
            decompressing_in.position() = decompressing_in.buffer().end();
            decompressing_in.next();
        }
        watch.stop();
        runs.push_back(watch.elapsed());
    }

    /// MIN / MAX / AVG
    UInt64 min = *std::min_element(runs.begin(), runs.end());
    UInt64 max = *std::max_element(runs.begin(), runs.end());
    UInt64 avg = std::accumulate(runs.begin(), runs.end(), UInt64(0)) / runs.size();
    std::cout << min << " " << max << " " << avg << '\n';

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true);
    return DB::getCurrentExceptionCode();
}
