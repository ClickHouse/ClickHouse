#include <random>
#include <gtest/gtest.h>
#include <Functions/GatherUtils/Algorithms.h>

using namespace DB::GatherUtils;


auto uni_int_dist(int min, int max)
{
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<> dist(min, max);
    return std::make_pair(dist, mt);
}

template<class T>
void arrayInit(T* elements_to_have, size_t nb_elements_to_have, T* array_elements, size_t array_size, bool all_elements_present)
{
    for (size_t i = 0; i < array_size; ++i)
    {
        array_elements[i] = i;
    }
    auto [dist, gen] = uni_int_dist(0, array_size - 1);
    for (size_t i = 0; i < nb_elements_to_have; ++i)
    {
        elements_to_have[i] = array_elements[dist(gen)];
    }
    if (!all_elements_present)
    {
        /// make one element to be searched for missing from the target array
        elements_to_have[nb_elements_to_have - 1] = array_size + 1;
    }
}

void nullMapInit(UInt8 * null_map, size_t null_map_size, size_t nb_null_elements)
{
    /// -2 to keep the last element of the array non-null
    auto [dist, gen] = uni_int_dist(0, null_map_size - 2);
    for (size_t i = 0; i < null_map_size; ++i)
    {
        null_map[i] = 0;
    }
    for (size_t i = 0; i < null_map_size - 1 && i < nb_null_elements; ++i)
    {
        null_map[dist(gen)] = 1;
    }
}

template<class T>
bool testHasAll(size_t nb_elements_to_have, size_t array_size, bool with_null_maps, bool all_elements_present)
{
    auto array_elements = std::make_unique<T[]>(array_size);
    auto elements_to_have = std::make_unique<T[]>(nb_elements_to_have);

    std::unique_ptr<UInt8[]> first_nm = nullptr, second_nm = nullptr;
    if (with_null_maps)
    {
        first_nm = std::make_unique<UInt8[]>(array_size);
        second_nm = std::make_unique<UInt8[]>(nb_elements_to_have);
        /// add a null to elements to have, but not to the target array, making the answer negative
        nullMapInit(first_nm.get(), array_size, 0);
        nullMapInit(second_nm.get(), nb_elements_to_have, 1);
    }

    arrayInit(elements_to_have.get(), nb_elements_to_have, array_elements.get(), array_size, all_elements_present);

    NumericArraySlice<T> first = {array_elements.get(), array_size};
    NumericArraySlice<T> second = {elements_to_have.get(), nb_elements_to_have};

    /// check whether all elements of the second array are also elements of the first array, overloaded for various combinations of types.
    return sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<T>, NumericArraySlice<T>, sliceEqualElements<T,T> >(
        first, second, first_nm.get(), second_nm.get());
}

TEST(HasAll, integer)
{
    bool test1 = testHasAll<int>(4, 100, false, true);
    bool test2 = testHasAll<int>(4, 100, false, false);
    bool test3 = testHasAll<int>(100, 4096, false, true);
    bool test4 = testHasAll<int>(100, 4096, false, false);

    ASSERT_EQ(test1, true);
    ASSERT_EQ(test2, false);
    ASSERT_EQ(test3, true);
    ASSERT_EQ(test4, false);
}


TEST(HasAll, int64)
{
    bool test1 = testHasAll<int64_t>(2, 100, false, true);
    bool test2 = testHasAll<int64_t>(2, 100, false, false);
    bool test3 = testHasAll<int64_t>(100, 4096, false, true);
    bool test4 = testHasAll<int64_t>(100, 4096, false, false);

    ASSERT_EQ(test1, true);
    ASSERT_EQ(test2, false);
    ASSERT_EQ(test3, true);
    ASSERT_EQ(test4, false);
}

TEST(HasAll, int16)
{
    bool test1 = testHasAll<int16_t>(2, 100, false, true);
    bool test2 = testHasAll<int16_t>(2, 100, false, false);
    bool test3 = testHasAll<int16_t>(100, 4096, false, true);
    bool test4 = testHasAll<int16_t>(100, 4096, false, false);

    ASSERT_EQ(test1, true);
    ASSERT_EQ(test2, false);
    ASSERT_EQ(test3, true);
    ASSERT_EQ(test4, false);
}

TEST(HasAll, int8)
{
    bool test1 = testHasAll<int8_t>(2, 100, false, true);
    bool test2 = testHasAll<int8_t>(2, 100, false, false);
    bool test3 = testHasAll<int8_t>(50, 125, false, true);
    bool test4 = testHasAll<int8_t>(50, 125, false, false);

    ASSERT_EQ(test1, true);
    ASSERT_EQ(test2, false);
    ASSERT_EQ(test3, true);
    ASSERT_EQ(test4, false);
}

TEST(HasAllSingleNullElement, all)
{
    bool test1 = testHasAll<int>(4, 100, true, true);
    bool test2 = testHasAll<int64_t>(4, 100, true, true);
    bool test3 = testHasAll<int16_t>(4, 100, true, true);
    bool test4 = testHasAll<int8_t>(4, 100, true, true);

    ASSERT_EQ(test1, false);
    ASSERT_EQ(test2, false);
    ASSERT_EQ(test3, false);
    ASSERT_EQ(test4, false);
}
