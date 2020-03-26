#include <gtest/gtest.h>

#include <Disks/IDisk.h>


TEST(DiskTest, parentPath)
{
    EXPECT_EQ("", DB::parentPath("test_dir/"));
    EXPECT_EQ("test_dir/", DB::parentPath("test_dir/nested_dir/"));
    EXPECT_EQ("test_dir/", DB::parentPath("test_dir/nested_file"));
}


TEST(DiskTest, fileName)
{
    EXPECT_EQ("test_file", DB::fileName("test_file"));
    EXPECT_EQ("nested_file", DB::fileName("test_dir/nested_file"));
    EXPECT_EQ("", DB::fileName("test_dir/"));
    EXPECT_EQ("", DB::fileName(""));
}
