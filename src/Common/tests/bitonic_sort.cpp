#include <Common/config.h>
#include <iostream>

#if USE_OPENCL

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif
#include <ext/bit_cast.h>
#include <Common/Stopwatch.h>
#include <IO/ReadHelpers.h>
#include <Core/Defines.h>
#include <climits>
#include <algorithm>

#include "Common/BitonicSort.h"


using Key = cl_ulong;


/// Generates vector of size 8 for testing.
/// Vector contains max possible value, min possible value and duplicate values.
template <class Type>
static void generateTest(std::vector<Type>& data, Type min_value, Type max_value)
{
    int size = 10;

    data.resize(size);
    data[0] = 10;
    data[1] = max_value;
    data[2] = 10;
    data[3] = 20;
    data[4] = min_value;
    data[5] = min_value + 1;
    data[6] = max_value - 5;
    data[7] = 1;
    data[8] = 42;
    data[9] = max_value - 1;
}


static void check(const std::vector<size_t> & indices, bool reverse = true)
{
    std::vector<size_t> reference_indices{4, 5, 7, 0, 2, 3, 8, 6, 9, 1};
    if (reverse) std::reverse(reference_indices.begin(), reference_indices.end());

    bool success = true;
    for (size_t index = 0; index < reference_indices.size(); ++index)
    {
        if (indices[index] != reference_indices[index])
        {
            success = false;
            std::cerr << "Test failed. Reason: indices[" << index << "] = "
                      << indices[index] << ", it must be equal to " << reference_indices[index] << "\n";
        }
    }

    std::string order_description = reverse ? "descending" : "ascending";
    std::cerr << "Sorted " << order_description << " sequence. Result: " << (success ? "Ok." : "Fail!") << "\n";
}


template <class Type>
static void sortBitonicSortWithPodArrays(const std::vector<Type>& data,
                                         std::vector<size_t> & indices, bool ascending = true)
{
    DB::PaddedPODArray<Type> pod_array_data = DB::PaddedPODArray<Type>(data.size());
    DB::IColumn::Permutation pod_array_indices = DB::IColumn::Permutation(data.size());

    for (size_t index = 0; index < data.size(); ++index)
    {
        *(pod_array_data.data() + index) = data[index];
        *(pod_array_indices.data() + index) = index;
    }

    BitonicSort::getInstance().configure();
    BitonicSort::getInstance().sort(pod_array_data, pod_array_indices, ascending);

    for (size_t index = 0; index < data.size(); ++index)
        indices[index] = pod_array_indices[index];
}


template <class Type>
static void testBitonicSort(std::string test_name, Type min_value, Type max_value)
{
    std::cerr << test_name << std::endl;

    std::vector<Type> data;
    generateTest<Type>(data, min_value, max_value);

    std::vector<size_t> indices(data.size());

    sortBitonicSortWithPodArrays(data, indices, true);
    check(indices, false);

    sortBitonicSortWithPodArrays(data, indices, false);
    check(indices, true);
}


static void straightforwardTests()
{
    testBitonicSort<cl_char>("Test 01: cl_char.", CHAR_MIN, CHAR_MAX);
    testBitonicSort<cl_uchar>("Test 02: cl_uchar.", 0, UCHAR_MAX);
    testBitonicSort<cl_short>("Test 03: cl_short.", SHRT_MIN, SHRT_MAX);
    testBitonicSort<cl_ushort>("Test 04: cl_ushort.", 0, USHRT_MAX);
    testBitonicSort<cl_int>("Test 05: cl_int.", INT_MIN, INT_MAX);
    testBitonicSort<cl_uint >("Test 06: cl_uint.", 0, UINT_MAX);
    testBitonicSort<cl_long >("Test 07: cl_long.", LONG_MIN, LONG_MAX);
    testBitonicSort<cl_ulong >("Test 08: cl_ulong.", 0, ULONG_MAX);
}


static void NO_INLINE sort1(Key * data, size_t size)
{
    std::sort(data, data + size);
}


static void NO_INLINE sort2(std::vector<Key> & data, std::vector<size_t> & indices)
{
    BitonicSort::getInstance().configure();

    sortBitonicSortWithPodArrays(data, indices);

    std::vector<Key> result(data.size());
    for (size_t index = 0; index < data.size(); ++index)
        result[index] = data[indices[index]];

    data = std::move(result);
}


int main(int argc, char ** argv)
{
    straightforwardTests();

    if (argc < 3)
    {
        std::cerr << "Not enough arguments were passed\n";
        return 1;
    }

    size_t n = DB::parse<size_t>(argv[1]);
    size_t method = DB::parse<size_t>(argv[2]);

    std::vector<Key> data(n);
    std::vector<size_t> indices(n);

    {
        Stopwatch watch;

        for (auto & elem : data)
            elem = static_cast<Key>(rand());

        for (size_t i = 0; i < n; ++i)
            indices[i] = i;

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
        for (const auto & index : indices)
            std::cerr << index << ' ';
        std::cerr << std::endl;
    }

    {
        Stopwatch watch;

        if (method == 1)    sort1(data.data(), n);
        if (method == 2)    sort2(data, indices);

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

        for (const auto & index : indices)
            std::cerr << index << ' ';
        std::cerr << std::endl;
    }

    return 0;
}

#else

int main()
{
    std::cerr << "Openc CL disabled.";

    return 0;
}

#endif
