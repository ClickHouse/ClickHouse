#include <gtest/gtest.h>
#include <Core/MySQL/MySQLGtid.h>

using namespace DB;


GTEST_TEST(GTIDSetsContains, Tests)
{
    GTIDSets gtid_set,
             contained1, contained2, contained3, contained4, contained5,
             not_contained1, not_contained2, not_contained3, not_contained4, not_contained5, not_contained6;

    gtid_set.parse("2174B383-5441-11E8-B90A-C80AA9429562:1-3:11:47-49, FBC30C64-F8C9-4DDF-8CDD-066208EB433B:1-19:47-49:60");
    contained1.parse("2174B383-5441-11E8-B90A-C80AA9429562:1-3:11:47-49, FBC30C64-F8C9-4DDF-8CDD-066208EB433B:1-19:47-49:60");
    contained2.parse("2174B383-5441-11E8-B90A-C80AA9429562:2-3:11:47-49");
    contained3.parse("2174B383-5441-11E8-B90A-C80AA9429562:11");
    contained4.parse("FBC30C64-F8C9-4DDF-8CDD-066208EB433B:2-16:47-49:60");
    contained5.parse("FBC30C64-F8C9-4DDF-8CDD-066208EB433B:60");

    not_contained1.parse("2174B383-5441-11E8-B90A-C80AA9429562:1-3:11:47-50, FBC30C64-F8C9-4DDF-8CDD-066208EB433B:1-19:47-49:60");
    not_contained2.parse("2174B383-5441-11E8-B90A-C80AA9429562:0-3:11:47-49");
    not_contained3.parse("2174B383-5441-11E8-B90A-C80AA9429562:99");
    not_contained4.parse("FBC30C64-F8C9-4DDF-8CDD-066208EB433B:2-16:46-49:60");
    not_contained5.parse("FBC30C64-F8C9-4DDF-8CDD-066208EB433B:99");
    not_contained6.parse("2174B383-5441-11E8-B90A-C80AA9429562:1-3:11:47-49, FBC30C64-F8C9-4DDF-8CDD-066208EB433B:1-19:47-49:60, 00000000-0000-0000-0000-000000000000");

    ASSERT_TRUE(gtid_set.contains(contained1));
    ASSERT_TRUE(gtid_set.contains(contained2));
    ASSERT_TRUE(gtid_set.contains(contained3));
    ASSERT_TRUE(gtid_set.contains(contained4));
    ASSERT_TRUE(gtid_set.contains(contained5));

    ASSERT_FALSE(gtid_set.contains(not_contained1));
    ASSERT_FALSE(gtid_set.contains(not_contained2));
    ASSERT_FALSE(gtid_set.contains(not_contained3));
    ASSERT_FALSE(gtid_set.contains(not_contained4));
    ASSERT_FALSE(gtid_set.contains(not_contained5));
    ASSERT_FALSE(gtid_set.contains(not_contained6));
}
