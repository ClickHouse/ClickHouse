#include <iomanip>
#include <iostream>

#include <IO/LZMADeflatingWriteBuffer.h>
#include <IO/LZMAInflatingReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/Stopwatch.h>

int main(int, char **)
try
{
    std::cout << std::fixed << std::setprecision(2);

    size_t n = 10000000;
    Stopwatch stopwatch;

    {
        auto buf
            = std::make_unique<DB::WriteBufferFromFile>("test_lzma_buffers.xz", DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_TRUNC);
        DB::LZMADeflatingWriteBuffer lzma_buf(std::move(buf), /*compression level*/ 3);

        stopwatch.restart();
        for (size_t i = 0; i < n; ++i)
        {
            DB::writeIntText(i, lzma_buf);
            DB::writeChar('\t', lzma_buf);
        }
        lzma_buf.finish();

        stopwatch.stop();

        std::cout << "Writing done. Elapsed: " << stopwatch.elapsedSeconds() << " s."
                  << ", " << (lzma_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s" << std::endl;
    }

    {
        auto buf = std::make_unique<DB::ReadBufferFromFile>("test_lzma_buffers.xz");
        DB::LZMAInflatingReadBuffer lzma_buf(std::move(buf));

        stopwatch.restart();
        for (size_t i = 0; i < n; ++i)
        {
            size_t x;
            DB::readIntText(x, lzma_buf);
            lzma_buf.ignore();

            if (x != i)
                throw DB::Exception("Failed!, read: " + std::to_string(x) + ", expected: " + std::to_string(i), 0);
        }
        stopwatch.stop();
        std::cout << "Reading done. Elapsed: " << stopwatch.elapsedSeconds() << " s."
                  << ", " << (lzma_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s" << std::endl;
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
