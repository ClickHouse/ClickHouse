#include <gtest/gtest.h>

#include <Functions/GatherUtils/Algorithms.h>

using namespace DB::GatherUtils;


template<class T>
void array_init(T* elements_to_have, size_t elements_to_have_count, T* set_elements, size_t set_size, bool expected_output) {
    for (T i = 0; i < set_size; ++i)
    {
        set_elements[i] = i;
    }
    for (T i = 0; i < elements_to_have_count; ++i)
    {
        elements_to_have[i] = set_elements[std::rand() % set_size];
    }
    if (!expected_output)
    {
        // make one element to be searched for missing from the target set
        elements_to_have[elements_to_have_count - 1] = set_size + 1;
    }
}

void null_map_init(UInt8 * null_map, size_t null_map_size, size_t null_elements_count)
{
    for (int i = 0; i < null_map_size; ++i)
    {
        null_map[i] = 0;
    }
    for (int i = 0; i < null_map_size - 1 && i < null_elements_count; ++i)
    {
        null_map[std::rand() % null_map_size - 1] = 1;
    }
}

template<class T>
bool testHasAll(size_t elements_to_have_count, size_t set_size, bool have_null_map, bool expected_output)
{
    T * set_elements = new T[set_size];
    T * elements_to_have = new T[elements_to_have_count];

    UInt8 * first_nm = nullptr, * second_nm = nullptr;
    if (have_null_map)
    {
        first_nm = new UInt8[set_size];
        second_nm = new UInt8[elements_to_have_count];
        null_map_init(first_nm, set_size, 5);
        null_map_init(second_nm, elements_to_have_count, 2);
    }

    array_init(elements_to_have, elements_to_have_count, set_elements, set_size, expected_output);

    NumericArraySlice<T> first = {set_elements, set_size};
    NumericArraySlice<T> second = {elements_to_have,  elements_to_have_count};

    /// Check whether all elements of the second array are also elements of the first array, overloaded for various combinations of types.
    return sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<T>, NumericArraySlice<T>, sliceEqualElements<T,T> >(
        first, second, first_nm, second_nm);
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
