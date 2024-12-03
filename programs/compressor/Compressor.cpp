#include <iostream>
#include <optional>
#include <boost/program_options.hpp>
#include <boost/algorithm/string/join.hpp>

#include <Common/Exception.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
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
            throw DB::Exception(DB::ErrorCodes::TOO_LARGE_SIZE_COMPRESSED, "Too large size_compressed. Most likely corrupted data.");

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
    using namespace DB;
    namespace po = boost::program_options;

    bool print_stacktrace = false;
    try
    {
        po::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
        desc.add_options()
            ("help,h", "produce help message")
            ("input", po::value<std::string>()->value_name("INPUT"), "input file")
            ("output", po::value<std::string>()->value_name("OUTPUT"), "output file")
            ("decompress,d", "decompress")
            ("offset-in-compressed-file", po::value<size_t>()->default_value(0ULL), "offset to the compressed block (i.e. physical file offset)")
            ("offset-in-decompressed-block", po::value<size_t>()->default_value(0ULL), "offset to the decompressed block (i.e. virtual offset)")
            ("block-size,b", po::value<unsigned>()->default_value(DBMS_DEFAULT_BUFFER_SIZE), "compress in blocks of specified size")
            ("hc", "use LZ4HC instead of LZ4")
            ("zstd", "use ZSTD instead of LZ4")
            ("codec", po::value<std::vector<std::string>>()->multitoken(), "use codecs combination instead of LZ4")
            ("level", po::value<int>(), "compression level for codecs specified via flags")
            ("none", "use no compression instead of LZ4")
            ("stat", "print block statistics of compressed data")
            ("stacktrace", "print stacktrace of exception")
        ;

        po::positional_options_description positional_desc;
        positional_desc.add("input", 1);
        positional_desc.add("output", 1);

        po::variables_map options;
        po::store(po::command_line_parser(argc, argv).options(desc).positional(positional_desc).run(), options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << argv[0] << " [options] < INPUT > OUTPUT" << std::endl;
            std::cout << "Usage: " << argv[0] << " [options] INPUT OUTPUT" << std::endl;
            std::cout << desc << std::endl;
            std::cout << "\nSee also: https://clickhouse.com/docs/en/operations/utilities/clickhouse-compressor/\n";
            return 0;
        }

        bool decompress = options.count("decompress");
        bool use_lz4hc = options.count("hc");
        bool use_zstd = options.count("zstd");
        bool stat_mode = options.count("stat");
        bool use_none = options.count("none");
        print_stacktrace = options.count("stacktrace");
        unsigned block_size = options["block-size"].as<unsigned>();
        std::vector<std::string> codecs;
        if (options.count("codec"))
            codecs = options["codec"].as<std::vector<std::string>>();

        if ((use_lz4hc || use_zstd || use_none) && !codecs.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong options, codec flags like --zstd and --codec options are mutually exclusive");

        if (!codecs.empty() && options.count("level"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong options, --level is not compatible with --codec list");

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
            auto ast = parseQuery(codec_parser, "(" + codecs_line + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
            codec = CompressionCodecFactory::instance().get(ast, nullptr);
        }
        else
            codec = CompressionCodecFactory::instance().get(method_family, level);


        std::unique_ptr<ReadBufferFromFileBase> rb;
        std::unique_ptr<WriteBufferFromFileBase> wb;

        if (options.count("input"))
            rb = std::make_unique<ReadBufferFromFile>(options["input"].as<std::string>());
        else
            rb = std::make_unique<ReadBufferFromFileDescriptor>(STDIN_FILENO);

        if (options.count("output"))
            wb = std::make_unique<WriteBufferFromFile>(options["output"].as<std::string>());
        else
            wb = std::make_unique<WriteBufferFromFileDescriptor>(STDOUT_FILENO);

        if (stat_mode)
        {
            /// Output statistic for compressed file.
            checkAndWriteHeader(*rb, *wb);
        }
        else if (decompress)
        {
            /// Decompression

            size_t offset_in_compressed_file = options["offset-in-compressed-file"].as<size_t>();
            size_t offset_in_decompressed_block = options["offset-in-decompressed-block"].as<size_t>();

            if (offset_in_compressed_file || offset_in_decompressed_block)
            {
                CompressedReadBufferFromFile compressed_file(std::move(rb));
                compressed_file.seek(offset_in_compressed_file, offset_in_decompressed_block);
                copyData(compressed_file, *wb);
            }
            else
            {
                CompressedReadBuffer from(*rb);
                copyData(from, *wb);
            }
        }
        else
        {
            /// Compression
            CompressedWriteBuffer to(*wb, codec, block_size);
            copyData(*rb, to);
            to.finalize();
        }
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(print_stacktrace) << '\n';
        return getCurrentExceptionCode();
    }

    return 0;
}
