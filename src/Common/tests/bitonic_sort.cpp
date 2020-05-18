#include <Common/config.h>
#include <iostream>

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


/// Generates vector of size 8 for testing.
/// Vector contains max possible value, min possible value and duplicate values.
template <class Type>
static void generateTest(std::vector<Type> & data, Type min_value, Type max_value)
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
static void sortBitonicSortWithPodArrays(const std::vector<Type> & data, std::vector<size_t> & indices, bool ascending = true)
{
    DB::PaddedPODArray<Type> pod_array_data = DB::PaddedPODArray<Type>(data.size());
    DB::IColumn::Permutation pod_array_indices = DB::IColumn::Permutation(data.size());

    for (size_t index = 0; index < data.size(); ++index)
    {
        *(pod_array_data.data() + index) = data[index];
        *(pod_array_indices.data() + index) = index;
    }

    BitonicSort::getInstance().sort(pod_array_data, pod_array_indices, ascending);

    for (size_t index = 0; index < data.size(); ++index)
        indices[index] = pod_array_indices[index];
}


template <class Type>
static void testBitonicSort(const std::string & test_name, Type min_value, Type max_value)
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
    testBitonicSort<DB::Int8>("Test 01: Int8.", CHAR_MIN, CHAR_MAX);
    testBitonicSort<DB::UInt8>("Test 02: UInt8.", 0, UCHAR_MAX);
    testBitonicSort<DB::Int16>("Test 03: Int16.", SHRT_MIN, SHRT_MAX);
    testBitonicSort<DB::UInt16>("Test 04: UInt16.", 0, USHRT_MAX);
    testBitonicSort<DB::Int32>("Test 05: Int32.", INT_MIN, INT_MAX);
    testBitonicSort<DB::UInt32>("Test 06: UInt32.", 0, UINT_MAX);
    testBitonicSort<DB::Int64>("Test 07: Int64.", LONG_MIN, LONG_MAX);
    testBitonicSort<DB::UInt64>("Test 08: UInt64.", 0, ULONG_MAX);
}


template <typename T>
static void bitonicSort(std::vector<T> & data)
{
    size_t size = data.size();
    std::vector<size_t> indices(size);
    for (size_t i = 0; i < size; ++i)
        indices[i] = i;

    sortBitonicSortWithPodArrays(data, indices);

    std::vector<T> result(size);
    for (size_t i = 0; i < size; ++i)
        result[i] = data[indices[i]];

    data = std::move(result);
}


template <typename T>
static bool checkSort(const std::vector<T> & data, size_t size)
{
    std::vector<T> copy1(data.begin(), data.begin() + size);
    std::vector<T> copy2(data.begin(), data.begin() + size);

    std::sort(copy1.data(), copy1.data() + size);
    bitonicSort<T>(copy2);

    for (size_t i = 0; i < size; ++i)
        if (copy1[i] != copy2[i])
            return false;

    return true;
}


int main()
{
    BitonicSort::getInstance().configure();

    straightforwardTests();

    size_t size = 1100;
    std::vector<int> data(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = rand();

    for (size_t i = 0; i < 128; ++i)
    {
        if (!checkSort<int>(data, i))
        {
            std::cerr << "fail at length " << i << std::endl;
            return 1;
        }
    }

    for (size_t i = 128; i < size; i += 7)
    {
        if (!checkSort<int>(data, i))
        {
            std::cerr << "fail at length " << i << std::endl;
            return 1;
        }
    }

    return 0;
}
