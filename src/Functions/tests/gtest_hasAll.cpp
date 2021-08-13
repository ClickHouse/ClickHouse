#include <gtest/gtest.h>

#include <Functions/GatherUtils/Algorithms.h>

using namespace DB::GatherUtils;



template<class T>
void array_init(T* first, size_t first_size, T* second, size_t second_size, bool expected_return) {
    T i = 0;
    for (; i < second_size; i++) {
        second[i] = i;
    }
    for (i=0; i < first_size; i++) {
        first[i] = second[std::rand()%second_size];
    }
    // Set one element different from
    if (!expected_return) {
        first[first_size-1] = second_size+1;
    }
}

void null_map_init(UInt8 * null_map, size_t null_map_size, size_t nb_elem) {
    for (int i =0; i < null_map_size-1 && i < nb_elem; i++) {
        null_map[std::rand()%null_map_size-1] = 1;
    }
}

template<class T>
bool test_hasAll(size_t first_size, size_t second_size, bool have_null_map, bool expected_output) {
    T* first_data  = new T [first_size];
    T* second_data  = new T [second_size];

    UInt8 *first_nm = nullptr, *second_nm = nullptr;
    if (have_null_map) {
        first_nm = new UInt8 [first_size];
        second_nm = new UInt8 [second_size];
        null_map_init(first_nm, first_size, 5);
        null_map_init(second_nm, second_size, 2);
    }

    array_init(first_data, first_size, second_data, second_size, expected_output);

    NumericArraySlice<T> first   = {first_data,  first_size};
    NumericArraySlice<T> second  = {second_data, second_size};

    // Test
    /// Check if all first array are elements from second array, overloaded for various combinations of types.
    return sliceHasImplAnyAll<ArraySearchType::All, NumericArraySlice<T>, NumericArraySlice<T>, sliceEqualElements<T,T> >(first, second, nullptr, nullptr);
}

TEST(HasAll, integer)
{
    bool test1 = test_hasAll<int>(4, 100, false, true);
    bool test2 = test_hasAll<int>(4, 100, false, false);
    bool test3 = test_hasAll<int>(100, 4096, false, true);
    bool test4 = test_hasAll<int>(100, 4096, false, false);

    ASSERT_EQ(test1, true);
    ASSERT_EQ(test2, false);
    ASSERT_EQ(test3, true);
    ASSERT_EQ(test4, false);
}


TEST(HasAll, int64)
{
    bool test1 = test_hasAll<int64_t>(2, 100, false, true);
    bool test2 = test_hasAll<int64_t>(2, 100, false, false);
    bool test3 = test_hasAll<int64_t>(100, 4096, false, true);
    bool test4 = test_hasAll<int64_t>(100, 4096, false, false);

    ASSERT_EQ(test1, true);
    ASSERT_EQ(test2, false);
    ASSERT_EQ(test3, true);
    ASSERT_EQ(test4, false);
}

TEST(HasAll, int16)
{
    bool test1 = test_hasAll<int16_t>(2, 100, false, true);
    bool test2 = test_hasAll<int16_t>(2, 100, false, false);
    bool test3 = test_hasAll<int16_t>(100, 4096, false, true);
    bool test4 = test_hasAll<int16_t>(100, 4096, false, false);

    ASSERT_EQ(test1, true);
    ASSERT_EQ(test2, false);
    ASSERT_EQ(test3, true);
    ASSERT_EQ(test4, false);
}

TEST(HasAll, int8)
{
    bool test1 = test_hasAll<int8_t>(2, 100, false, true);
    bool test2 = test_hasAll<int8_t>(2, 100, false, false);
    bool test3 = test_hasAll<int8_t>(50, 125, false, true);
    bool test4 = test_hasAll<int8_t>(50, 125, false, false);

    ASSERT_EQ(test1, true);
    ASSERT_EQ(test2, false);
    ASSERT_EQ(test3, true);
    ASSERT_EQ(test4, false);
}
