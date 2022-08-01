#include <string>

#include <iostream>
#include <fstream>
#include <iomanip>

#include <Common/Stopwatch.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


int main(int, char **)
{
    try
    {
        std::cout << std::fixed << std::setprecision(2);

        size_t n = 100000000;
        Stopwatch stopwatch;

        {
            DB::WriteBufferFromFile buf("test1", DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_TRUNC);
            DB::CompressedWriteBuffer compressed_buf(buf);

            stopwatch.restart();
            for (size_t i = 0; i < n; ++i)
            {
                DB::writeIntText(i, compressed_buf);
                DB::writeChar('\t', compressed_buf);
            }
            compressed_buf.finalize();
            stopwatch.stop();
            std::cout << "Writing done (1). Elapsed: " << stopwatch.elapsedSeconds()
                << ", " << (compressed_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s"
                << std::endl;
        }

        {
            DB::ReadBufferFromFile buf("test1");
            DB::CompressedReadBuffer compressed_buf(buf);

            stopwatch.restart();
            for (size_t i = 0; i < n; ++i)
            {
                size_t x;
                DB::readIntText(x, compressed_buf);
                compressed_buf.ignore();

                if (x != i)
                {
                    throw DB::Exception(0, "Failed!, read: {}, expected: {}", x, i);
                }
            }
            stopwatch.stop();
            std::cout << "Reading done (1). Elapsed: " << stopwatch.elapsedSeconds()
                << ", " << (compressed_buf.count() / stopwatch.elapsedSeconds() / 1000000) << " MB/s"
                << std::endl;
        }
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
