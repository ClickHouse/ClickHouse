#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif
#include <ext/bit_cast.h>
#include <Common/RadixSort.h>
#include <Common/Stopwatch.h>
#include <IO/ReadHelpers.h>
#include <Core/Defines.h>

using Key = double;

void NO_INLINE sort1(Key * data, size_t size)
{
    std::sort(data, data + size);
}

void NO_INLINE sort2(Key * data, size_t size)
{
    radixSort(data, size);
}

void NO_INLINE sort3(Key * data, size_t size)
{
    std::sort(data, data + size, [](Key a, Key b)
    {
        return RadixSortFloatTransform<uint32_t>::forward(ext::bit_cast<uint32_t>(a))
            < RadixSortFloatTransform<uint32_t>::forward(ext::bit_cast<uint32_t>(b));
    });
}


int main(int argc, char ** argv)
{
    if (argc < 3)
    {
        std::cerr << "Usage: program n method\n";
        return 1;
    }

    size_t n = DB::parse<size_t>(argv[1]);
    size_t method = DB::parse<size_t>(argv[2]);

    std::vector<Key> data(n);

//    srand(time(nullptr));

    {
        Stopwatch watch;

        for (auto & elem : data)
            elem = rand();

        watch.stop();
        double elapsed = watch.elapsedSeconds();
        std::cerr
            << "Filled in " << elapsed
            << " (" << n / elapsed << " elem/sec., "
            << n * sizeof(Key) / elapsed / 1048576 << " MB/sec.)"
            << std::endl;
    }

    if (n <= 100)
    {
        std::cerr << std::endl;
        for (const auto & elem : data)
            std::cerr << elem << ' ';
        std::cerr << std::endl;
    }


    {
        Stopwatch watch;

        if (method == 1)    sort1(data.data(), n);
        if (method == 2)    sort2(data.data(), n);
        if (method == 3)    sort3(data.data(), n);

        watch.stop();
        double elapsed = watch.elapsedSeconds();
        std::cerr
            << "Sorted in " << elapsed
            << " (" << n / elapsed << " elem/sec., "
            << n * sizeof(Key) / elapsed / 1048576 << " MB/sec.)"
            << std::endl;
    }

    {
        Stopwatch watch;

        size_t i = 1;
        while (i < n)
        {
            if (!(data[i - 1] <= data[i]))
                break;
            ++i;
        }

        watch.stop();
        double elapsed = watch.elapsedSeconds();
        std::cerr
            << "Checked in " << elapsed
            << " (" << n / elapsed << " elem/sec., "
            << n * sizeof(Key) / elapsed / 1048576 << " MB/sec.)"
            << std::endl
            << "Result: " << (i == n ? "Ok." : "Fail!") << std::endl;
    }

    if (n <= 1000)
    {
        std::cerr << std::endl;

        std::cerr << data[0] << ' ';
        for (size_t i = 1; i < n; ++i)
        {
            if (!(data[i - 1] <= data[i]))
                std::cerr << "*** ";
            std::cerr << data[i] << ' ';
        }

        std::cerr << std::endl;
    }

    return 0;
}
