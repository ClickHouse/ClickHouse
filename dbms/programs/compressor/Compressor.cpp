#include <iostream>

#include <boost/program_options.hpp>

#include <Common/Exception.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int TOO_LARGE_SIZE_COMPRESSED;
    }
}


namespace
{

/// Outputs sizes of uncompressed and compressed blocks for compressed file.
void checkAndWriteHeader(DB::ReadBuffer & in, DB::WriteBuffer & out)
{
    while (!in.eof())
    {
        in.ignore(16);    /// checksum

        char header[COMPRESSED_BLOCK_HEADER_SIZE];
        in.readStrict(header, COMPRESSED_BLOCK_HEADER_SIZE);

        UInt32 size_compressed = unalignedLoad<UInt32>(&header[1]);

        if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
            throw DB::Exception("Too large size_compressed. Most likely corrupted data.", DB::ErrorCodes::TOO_LARGE_SIZE_COMPRESSED);

        UInt32 size_decompressed = unalignedLoad<UInt32>(&header[5]);

        DB::writeText(size_decompressed, out);
        DB::writeChar('\t', out);
        DB::writeText(size_compressed, out);
        DB::writeChar('\n', out);

        in.ignore(size_compressed - COMPRESSED_BLOCK_HEADER_SIZE);
    }
}

}


int mainEntryClickHouseCompressor(int argc, char ** argv)
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("decompress,d", "decompress")
        ("block-size,b", boost::program_options::value<unsigned>()->default_value(DBMS_DEFAULT_BUFFER_SIZE), "compress in blocks of specified size")
        ("hc", "use LZ4HC instead of LZ4")
        ("zstd", "use ZSTD instead of LZ4")
        ("level", "compression level")
        ("none", "use no compression instead of LZ4")
        ("stat", "print block statistics of compressed data")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " [options] < in > out" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    try
    {
        bool decompress = options.count("decompress");
        bool use_lz4hc = options.count("hc");
        bool use_zstd = options.count("zstd");
        bool stat_mode = options.count("stat");
        bool use_none = options.count("none");
        unsigned block_size = options["block-size"].as<unsigned>();

        DB::CompressionMethod method = DB::CompressionMethod::LZ4;

        if (use_lz4hc)
            method = DB::CompressionMethod::LZ4HC;
        else if (use_zstd)
            method = DB::CompressionMethod::ZSTD;
        else if (use_none)
            method = DB::CompressionMethod::NONE;

        DB::CompressionSettings settings(method, options.count("level") > 0 ? options["level"].as<int>() : DB::CompressionSettings::getDefaultLevel(method));

        DB::ReadBufferFromFileDescriptor rb(STDIN_FILENO);
        DB::WriteBufferFromFileDescriptor wb(STDOUT_FILENO);

        if (stat_mode)
        {
            /// Output statistic for compressed file.
            checkAndWriteHeader(rb, wb);
        }
        else if (decompress)
        {
            /// Decompression
            DB::CompressedReadBuffer from(rb);
            DB::copyData(from, wb);
        }
        else
        {
            /// Compression
            DB::CompressedWriteBuffer to(wb, settings, block_size);
            DB::copyData(rb, to);
        }
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true);
        return DB::getCurrentExceptionCode();
    }

    return 0;
}
