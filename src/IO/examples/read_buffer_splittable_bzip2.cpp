#include <cstdint>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <bzlib.h>
#include <Core/Defines.h>
#include <IO/BoundedReadBuffer.h>
#include <IO/Bzip2ReadBuffer.h>
#include <IO/ParallelBzip2ReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/SharedThreadPools.h>
#include <IO/SplittableBzip2ReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Poco/ConsoleChannel.h>

using namespace DB;
namespace fs = std::filesystem;

static const String compressed_file = "/path/to/2.bz2";
static const String decompressed1_file = "/path/to/2.1.txt";
static const String decompressed2_file = "/path/to/2.2.txt";
static const String decompressed3_file = "/path/to/2.3.txt";
static constexpr size_t max_split_bytes = 2 * 1024 * 1024UL;
static constexpr size_t max_working_readers = 16;
static size_t file_size = 0;

using SplittableBzip2ReadBufferPtr = std::shared_ptr<SplittableBzip2ReadBuffer>;

static void decompressFromFile()
{
    auto in = std::make_unique<ReadBufferFromFile>(compressed_file);
    auto rb = std::make_unique<Bzip2ReadBuffer>(std::move(in));
    auto out = std::make_unique<WriteBufferFromFile>(decompressed1_file);
    copyData(*rb, *out);
}

static void decompressFromSplits()
{
    std::vector<SplittableBzip2ReadBufferPtr> decompressors;
    size_t offset = 0;
    while (offset < file_size)
    {
        auto in = std::make_unique<BoundedReadBuffer>(std::make_unique<ReadBufferFromFile>(compressed_file));
        in->seek(offset, SEEK_SET);
        in->setReadUntilPosition(std::min(offset + max_split_bytes, file_size));
        offset += max_split_bytes;

        auto decompressor = std::make_shared<SplittableBzip2ReadBuffer>(std::move(in));
        decompressors.emplace_back(decompressor);
    }

    for (size_t i = 1; i < decompressors.size(); ++i)
    {
        auto adjuested_start = decompressors[i]->getAdjustedStart();
        if (adjuested_start.has_value())
        {
            auto & last_in = dynamic_cast<BoundedReadBuffer &>(decompressors[i - 1]->getWrappedReadBuffer());
            last_in.setReadUntilPosition(adjuested_start.value() - 5);
        }
        else
        {
            std::cout << "No adjuested start" << std::endl;
            return;
        }
    }

    for (const auto & decompressor : decompressors)
    {
        auto & in = dynamic_cast<BoundedReadBuffer &>(decompressor->getWrappedReadBuffer());
        std::cout << "range:" << in.getPosition() << " " << *in.getReadUntilPosition() << std::endl;
    }

    auto out = std::make_unique<WriteBufferFromFile>(decompressed2_file);
    for (auto & decompressor : decompressors)
    {
        copyData(*decompressor, *out);
    }
}

static void parallelDecompressFromSplits()
{
    getIOThreadPool().initialize(100, 0, 10000);
    auto in = std::make_unique<ReadBufferFromFilePRead>(compressed_file);
    auto rb = std::make_unique<ParallelBzip2ReadBuffer>(
        std::move(in),
        threadPoolCallbackRunner<void>(getIOThreadPool().get(), "ParallelRead"),
        max_working_readers,
        max_split_bytes,
        file_size);
    auto out = std::make_unique<WriteBufferFromFile>(decompressed3_file);
    copyData(*rb, *out);
}

int main()
{
    Poco::AutoPtr<Poco::ConsoleChannel> chan(new Poco::ConsoleChannel);
    Poco::Logger::root().setChannel(chan);
    Poco::Logger::root().setLevel("trace");

    fs::path path(compressed_file);
    try
    {
        file_size = fs::file_size(path);
    }
    catch (std::filesystem::filesystem_error & e)
    {
        std::cout << e.what() << '\n';
        return 1;
    }

    Stopwatch watch;

    watch.restart();
    decompressFromSplits();
    std::cout << "decompressFromSplits cost " << watch.elapsedSeconds() << " seconds" << std::endl;

    watch.restart();
    parallelDecompressFromSplits();
    std::cout << "parallelDecompressFromSplits cost " << watch.elapsedSeconds() << " seconds" << std::endl;

    watch.restart();
    decompressFromFile();
    std::cout << "decompressFromFile cost " << watch.elapsedSeconds() << " seconds" << std::endl;
    return 0;
}
