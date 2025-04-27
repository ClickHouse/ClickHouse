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
#include <Compression/ParallelCompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/getCompressionCodecForFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Parsers/parseQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Compression/CompressionFactory.h>
#include <Common/TerminalSize.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Core/Defines.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int TOO_LARGE_SIZE_COMPRESSED;
        extern const int BAD_ARGUMENTS;
    }
}

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}


namespace
{

/// Outputs method, sizes of uncompressed and compressed blocks for compressed file.
void checkAndWriteHeader(DB::ReadBuffer & in, DB::WriteBuffer & out)
{
    while (!in.eof())
    {
        UInt32 size_compressed;
        UInt32 size_decompressed;
        auto codec = DB::getCompressionCodecForFile(in, size_compressed, size_decompressed, true /* skip_to_next_block */);

        if (size_compressed > DBMS_MAX_COMPRESSED_SIZE)
            throw DB::Exception(DB::ErrorCodes::TOO_LARGE_SIZE_COMPRESSED, "Too large size_compressed. Most likely corrupted data.");

        DB::writeText(codec->getFullCodecDesc()->formatWithSecretsOneLine(), out);
        DB::writeChar('\t', out);
        DB::writeText(size_decompressed, out);
        DB::writeChar('\t', out);
        DB::writeText(size_compressed, out);
        DB::writeChar('\n', out);
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
            ("block-size,b", po::value<size_t>()->default_value(DBMS_DEFAULT_BUFFER_SIZE), "compress in blocks of specified size")
            ("hc", "use LZ4HC instead of LZ4")
            ("zstd", "use ZSTD instead of LZ4")
            ("deflate_qpl", "use deflate_qpl instead of LZ4")
            ("codec", po::value<std::vector<std::string>>()->multitoken(), "use codecs combination instead of LZ4")
            ("level", po::value<int>(), "compression level for codecs specified via flags")
            ("threads", po::value<size_t>()->default_value(1), "number of threads for parallel compression")
            ("none", "use no compression instead of LZ4")
            ("no-checksum-validation", "disable checksum validation")
            ("stat", "print block statistics of compressed data")
            ("stacktrace", "print stacktrace of exception")
        ;

        po::positional_options_description positional_desc;
        positional_desc.add("input", 1);
        positional_desc.add("output", 1);

        po::variables_map options;
        po::store(po::command_line_parser(argc, argv).options(desc).positional(positional_desc).run(), options);

        if (options.contains("help"))
        {
            std::cout << "Usage: " << argv[0] << " [options] < INPUT > OUTPUT" << std::endl;
            std::cout << "Usage: " << argv[0] << " [options] INPUT OUTPUT" << std::endl;
            std::cout << desc << std::endl;
            std::cout << "\nSee also: https://clickhouse.com/docs/en/operations/utilities/clickhouse-compressor/\n";
            return 0;
        }

        bool decompress = options.contains("decompress");
        bool use_lz4hc = options.contains("hc");
        bool use_zstd = options.contains("zstd");
        bool use_deflate_qpl = options.contains("deflate_qpl");
        bool stat_mode = options.contains("stat");
        bool use_none = options.contains("none");
        print_stacktrace = options.contains("stacktrace");
        size_t block_size = options["block-size"].as<size_t>();
        size_t num_threads = options["threads"].as<size_t>();
        std::vector<std::string> codecs;
        if (options.contains("codec"))
            codecs = options["codec"].as<std::vector<std::string>>();

        if ((use_lz4hc || use_zstd || use_deflate_qpl || use_none) && !codecs.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong options, codec flags like --zstd and --codec options are mutually exclusive");

        if (num_threads < 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid value of `threads` parameter");

        if (num_threads > 1 && decompress)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parallel mode is only implemented for compression (not for decompression)");

        if (!codecs.empty() && options.contains("level"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong options, --level is not compatible with --codec list");

        std::string method_family = "LZ4";

        if (use_lz4hc)
            method_family = "LZ4HC";
        else if (use_zstd)
            method_family = "ZSTD";
        else if (use_deflate_qpl)
            method_family = "DEFLATE_QPL";
        else if (use_none)
            method_family = "NONE";

        std::optional<int> level = std::nullopt;
        if (options.contains("level"))
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

        if (options.contains("input"))
            rb = std::make_unique<ReadBufferFromFile>(options["input"].as<std::string>());
        else
            rb = std::make_unique<ReadBufferFromFileDescriptor>(STDIN_FILENO);

        if (options.contains("output"))
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
                if (options.contains("no-checksum-validation"))
                    compressed_file.disableChecksumming();
                compressed_file.seek(offset_in_compressed_file, offset_in_decompressed_block);
                copyData(compressed_file, *wb);
            }
            else
            {
                CompressedReadBuffer from(*rb);
                if (options.contains("no-checksum-validation"))
                    from.disableChecksumming();
                copyData(from, *wb);
            }
        }
        else
        {
            /// Compression

            if (num_threads == 1)
            {
                CompressedWriteBuffer to(*wb, codec, block_size);
                copyData(*rb, to);
                to.finalize();
            }
            else
            {
                ThreadPool pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled, num_threads);
                ParallelCompressedWriteBuffer to(*wb, codec, block_size, num_threads, pool);
                copyData(*rb, to);
                to.finalize();
            }
        }

        wb->finalize();
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(print_stacktrace) << '\n';
        return getCurrentExceptionCode();
    }

    return 0;
}
