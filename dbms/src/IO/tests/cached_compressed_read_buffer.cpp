#include <iostream>
#include <iomanip>
#include <limits>

#include <IO/CachedCompressedReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>

#include <Common/Stopwatch.h>


int main(int argc, char ** argv)
{
    using namespace DB;

    if (argc < 2)
    {
        std::cerr << "Usage: program path\n";
        return 1;
    }

    try
    {
        UncompressedCache cache(1024);
        std::string path = argv[1];

        std::cerr << std::fixed << std::setprecision(3);

        size_t hits = 0;
        size_t misses = 0;

        {
            Stopwatch watch;
            CachedCompressedReadBuffer in(path, &cache, 0, 0);
            WriteBufferFromFile out("/dev/null");
            copyData(in, out);

            std::cerr << "Elapsed: " << watch.elapsedSeconds() << std::endl;
        }

        cache.getStats(hits, misses);
        std::cerr << "Hits: " << hits << ", misses: " << misses << std::endl;

        {
            Stopwatch watch;
            CachedCompressedReadBuffer in(path, &cache, 0, 0);
            WriteBufferFromFile out("/dev/null");
            copyData(in, out);

            std::cerr << "Elapsed: " << watch.elapsedSeconds() << std::endl;
        }

        cache.getStats(hits, misses);
        std::cerr << "Hits: " << hits << ", misses: " << misses << std::endl;
    }
    catch (const Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
