#include <iostream>
#include <string>

#include <Common/Exception.h>
#include <Common/Stopwatch.h>

#include <IO/ReadHelpers.h>
#include <IO/rapidgzip/RapidGzipReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/copyData.h>

int main(int /* argc */, char ** /* argv */)
try
{
    std::cout << std::fixed << std::setprecision(2);

    size_t n = 100000000;
    Stopwatch stopwatch;

    std::unique_ptr<DB::ReadBufferFromFile> buf;
    try
    {
        buf = std::make_unique<DB::ReadBufferFromFile>("test_zlib_buffers.gz");
    }
    catch (DB::ErrnoException & e)
    {
        if (e.getErrno() == ENOENT)
        {
            std::cerr << "File test_zlib_buffers.gz not found. You need to run the example with the `zlib_buffers` example first." << std::endl;
            return 1;
        }
        throw;
    }

    DB::RapidGzipReadBuffer inflating_buf(std::move(buf));

    stopwatch.restart();

    // auto out_buf = std::make_unique<DB::WriteBufferFromFile>("test_zlib_buffers", DB::DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_TRUNC);
    // DB::copyData(inflating_buf, *out_buf);

    for (size_t i = 0; i < n; ++i)
    {
        size_t x;
        DB::readIntText(x, inflating_buf);
        inflating_buf.ignore();

        if (x != i)
            throw DB::Exception(0, "Failed!, read: {}, expected: {}", x, i);
    }
    stopwatch.stop();
    std::cout << "Reading done. Elapsed: " << stopwatch.elapsedSeconds() << " s."
        << ", " << (inflating_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s"
        << std::endl;

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
