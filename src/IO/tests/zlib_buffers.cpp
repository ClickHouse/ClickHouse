#include <string>

#include <iostream>
#include <iomanip>

#include <Common/Stopwatch.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ZlibDeflatingWriteBuffer.h>
#include <IO/ZlibInflatingReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


int main(int, char **)
try
{
    std::cout << std::fixed << std::setprecision(2);

    size_t n = 100000000;
    Stopwatch stopwatch;

    {
        auto buf = std::make_unique<DB::WriteBufferFromFile>("test_zlib_buffers.gz", DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_TRUNC);
        DB::ZlibDeflatingWriteBuffer deflating_buf(std::move(buf), DB::CompressionMethod::Gzip, /* compression_level = */ 3);

        stopwatch.restart();
        for (size_t i = 0; i < n; ++i)
        {
            DB::writeIntText(i, deflating_buf);
            DB::writeChar('\t', deflating_buf);
        }
        deflating_buf.finish();

        stopwatch.stop();
        std::cout << "Writing done. Elapsed: " << stopwatch.elapsedSeconds() << " s."
            << ", " << (deflating_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s"
            << std::endl;
    }

    {
        auto buf = std::make_unique<DB::ReadBufferFromFile>("test_zlib_buffers.gz");
        DB::ZlibInflatingReadBuffer inflating_buf(std::move(buf), DB::CompressionMethod::Gzip);

        stopwatch.restart();
        for (size_t i = 0; i < n; ++i)
        {
            size_t x;
            DB::readIntText(x, inflating_buf);
            inflating_buf.ignore();

            if (x != i)
                throw DB::Exception("Failed!, read: " + std::to_string(x) + ", expected: " + std::to_string(i), 0);
        }
        stopwatch.stop();
        std::cout << "Reading done. Elapsed: " << stopwatch.elapsedSeconds() << " s."
            << ", " << (inflating_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s"
            << std::endl;
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
