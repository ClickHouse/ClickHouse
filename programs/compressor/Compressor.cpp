#include <iostream>
#include <optional>
#include <boost/program_options.hpp>
#include <boost/algorithm/string/join.hpp>

#include <Common/Exception.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Compression/CompressionFactory.h>
#include <Common/TerminalSize.h>
#include <Core/Defines.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int TOO_LARGE_SIZE_COMPRESSED;
        extern const int BAD_ARGUMENTS;
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

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseCompressor(int argc, char ** argv)
{
    using namespace DB;

    boost::program_options::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
    desc.add_options()
        ("help,h", "produce help message")
        ("decompress,d", "decompress")
        ("block-size,b", boost::program_options::value<unsigned>()->default_value(DBMS_DEFAULT_BUFFER_SIZE), "compress in blocks of specified size")
        ("hc", "use LZ4HC instead of LZ4")
        ("zstd", "use ZSTD instead of LZ4")
        ("codec", boost::program_options::value<std::vector<std::string>>()->multitoken(), "use codecs combination instead of LZ4")
        ("level", boost::program_options::value<int>(), "compression level for codecs specified via flags")
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
        std::vector<std::string> codecs;
        if (options.count("codec"))
            codecs = options["codec"].as<std::vector<std::string>>();

        if ((use_lz4hc || use_zstd || use_none) && !codecs.empty())
            throw Exception("Wrong options, codec flags like --zstd and --codec options are mutually exclusive", ErrorCodes::BAD_ARGUMENTS);

        if (!codecs.empty() && options.count("level"))
            throw Exception("Wrong options, --level is not compatible with --codec list", ErrorCodes::BAD_ARGUMENTS);

        std::string method_family = "LZ4";

        if (use_lz4hc)
            method_family = "LZ4HC";
        else if (use_zstd)
            method_family = "ZSTD";
        else if (use_none)
            method_family = "NONE";

        std::optional<int> level = std::nullopt;
        if (options.count("level"))
            level = options["level"].as<int>();

        CompressionCodecPtr codec;
        if (!codecs.empty())
        {
            ParserCodec codec_parser;

            std::string codecs_line = boost::algorithm::join(codecs, ",");
            auto ast = parseQuery(codec_parser, "(" + codecs_line + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
            codec = CompressionCodecFactory::instance().get(ast, nullptr);
        }
        else
            codec = CompressionCodecFactory::instance().get(method_family, level);


        ReadBufferFromFileDescriptor rb(STDIN_FILENO);
        WriteBufferFromFileDescriptor wb(STDOUT_FILENO);

        if (stat_mode)
        {
            /// Output statistic for compressed file.
            checkAndWriteHeader(rb, wb);
        }
        else if (decompress)
        {
            /// Decompression
            CompressedReadBuffer from(rb);
            copyData(from, wb);
        }
        else
        {
            /// Compression
            CompressedWriteBuffer to(wb, codec, block_size);
            copyData(rb, to);
        }
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true);
        return getCurrentExceptionCode();
    }

    return 0;
}
