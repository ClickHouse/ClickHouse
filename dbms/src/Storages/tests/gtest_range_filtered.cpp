#include <gtest/gtest.h>
#include <common/RangeFiltered.h>
#include <vector>
#include <set>


TEST(RangeFiltered, simple)
{
    std::vector<int> v;

    for (int i = 0; i < 10; ++i)
        v.push_back(i);

    auto v30 = createRangeFiltered([] (int i) { return i % 3 == 0; }, v);
    auto v31 = createRangeFiltered([] (int i) { return i % 3 != 0; }, v);

    for (const int & i : v30)
        ASSERT_EQ(i % 3, 0);

    for (const int & i : v31)
        ASSERT_NE(i % 3, 0);

    {
        auto it = v30.begin();
        ASSERT_EQ(*it, 0);

        auto it2 = std::next(it);
        ASSERT_EQ(*it2, 3);

        it = std::next(it2);
        ASSERT_EQ(*it, 6);
    }

    {
        auto it = std::next(v30.begin());
        ASSERT_EQ(*it, 3);

        *it = 2; /// it becomes invalid
        ASSERT_EQ(*(++it), 6); /// but iteration is sucessfull

        *v30.begin() = 1;
        ASSERT_EQ(*v30.begin(), 6);
    }
}
