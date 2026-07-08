#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ZstdDeflatingWriteBuffer.h>
#include <IO/ZstdInflatingReadBuffer.h>
#include <Common/Stopwatch.h>

#include <cerrno>
#include <cstdlib>
#include <iomanip>
#include <iostream>

#include <zstd.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}


int main(int argc, char ** argv)
try
{
    std::cout << std::fixed << std::setprecision(2);

    size_t n = 10000000;
    Stopwatch stopwatch;

    int compression_level = ZSTD_defaultCLevel();
    if (argc > 1)
    {
        errno = 0;
        char * pos_integer = argv[1];
        int uint_value = static_cast<int>(std::strtoul(argv[1], &pos_integer, 10));

        if (pos_integer == argv[1] + strlen(argv[1]) && errno != ERANGE && uint_value <= ZSTD_maxCLevel() && uint_value >= ZSTD_minCLevel())
            compression_level = uint_value;
        else
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Wrong compression level: '{}'. Expected an integer ({} <= N <= {})",
                argv[1],
                ZSTD_minCLevel(),
                ZSTD_maxCLevel());
    }

    {
        auto buf
            = std::make_unique<DB::WriteBufferFromFile>("test_zstd_buffers.zst", DB::DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_TRUNC);
        DB::ZstdDeflatingWriteBuffer zstd_buf(std::move(buf), /*compression level*/ compression_level);

        stopwatch.restart();
        for (size_t i = 0; i < n; ++i)
        {
            DB::writeIntText(i, zstd_buf);
            DB::writeChar('\t', zstd_buf);
        }
        zstd_buf.finalize();

        stopwatch.stop();

        std::cout << "Writing done. Elapsed: " << stopwatch.elapsedSeconds() << " s."
                  << ", " << (zstd_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s" << std::endl;
    }

    {
        auto buf = std::make_unique<DB::ReadBufferFromFile>("test_zstd_buffers.zst");
        DB::ZstdInflatingReadBuffer zstd_buf(std::move(buf));

        stopwatch.restart();
        for (size_t i = 0; i < n; ++i)
        {
            size_t x;
            DB::readIntText(x, zstd_buf);
            zstd_buf.ignore();

            if (x != i)
                throw DB::Exception(0, "Failed!, read: {}, expected: {}", x, i);
        }
        stopwatch.stop();
        std::cout << "Reading done. Elapsed: " << stopwatch.elapsedSeconds() << " s."
                  << ", " << (zstd_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s" << std::endl;
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
