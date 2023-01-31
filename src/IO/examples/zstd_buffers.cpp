#include <iomanip>
#include <iostream>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ZstdDeflatingWriteBuffer.h>
#include <IO/ZstdInflatingReadBuffer.h>
#include <Common/Stopwatch.h>


int main(int, char **)
try
{
    std::cout << std::fixed << std::setprecision(2);

    size_t n = 10000000;
    Stopwatch stopwatch;


    {
        auto buf
            = std::make_unique<DB::WriteBufferFromFile>("test_zstd_buffers.zst", DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_TRUNC);
        DB::ZstdDeflatingWriteBuffer zstd_buf(std::move(buf), /*compression level*/ 3);

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
                throw DB::Exception("Failed!, read: " + std::to_string(x) + ", expected: " + std::to_string(i), 0);
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
